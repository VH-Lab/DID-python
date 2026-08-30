import abc


class FileAccessError(FileNotFoundError):
    """A document's file exists in the record but cannot be opened.

    ``identifier`` carries MATLAB's error identifier for the same failure, as
    ``ValidationError`` does, so both languages can be branched on with the
    same strings. Subclasses FileNotFoundError so that callers written against
    the older behavior keep working.
    """

    def __init__(self, identifier, message):
        super().__init__(message)
        self.identifier = identifier


class Database(abc.ABC):
    def __init__(self, connection="", **kwargs):
        self.connection = connection
        self.version = None
        self.current_branch_id = ""
        self.frozen_branch_ids = []
        self.dbid = None
        self.preferences = {}
        self.debug = kwargs.get("debug", False)

    def __del__(self):
        # A destructor must never raise: the object may be only partially
        # constructed, and the interpreter may already be shutting down.
        try:
            self.close()
        except Exception:  # noqa: BLE001, S110 - see above
            pass

    def open(self):
        return self._open_db()

    def close(self):
        self._close_db()

    @abc.abstractmethod
    def _open_db(self):
        pass

    @abc.abstractmethod
    def _close_db(self):
        pass

    def all_branch_ids(self):
        return self._do_get_branch_ids()

    def add_branch(self, branch_id, parent_branch_id=None):
        """Add a branch, refusing the cases MATLAB refuses.

        This was a bare delegate behind a "validation logic would go here"
        placeholder. Two of MATLAB's three checks had a schema backstop, so
        they were refused but only as a raw sqlite3.IntegrityError with no
        indication of the cause: a duplicate BRANCH_ID by the UNIQUE
        constraint, and a missing parent by the FOREIGN KEY.

        The third had no backstop, and let two silent corruptions through:

        - `add_branch("")` created a real branch whose id is empty and made it
          current. Since "" is also the sentinel for *no* current branch, the
          next add_branch read that parent, converted it to NULL, and silently
          made the new branch a ROOT rather than its child.
        - `add_branch(42)` stored '42' (TEXT affinity coerces it) but set
          current_branch_id to the integer. SQLite never compares an integer
          equal to a text value, so the current branch then named nothing:
          get_doc_ids on it returned [] rather than raising.

        An empty or omitted PARENT_BRANCH_ID means the current branch, exactly
        as in MATLAB, whose isempty() covers both [] and ''. A branch with no
        parent -- a root -- is therefore made only when there is no current
        branch: on a fresh database, or after delete_branch drops the last
        one. set_branch cannot clear it (see there), so as in MATLAB there is
        no way to add a second root to a database that already has one.
        """
        if not parent_branch_id:
            parent_branch_id = self.current_branch_id

        # check_existence=False: this id is supposed NOT to exist yet.
        branch_id, branch_ids = self._validate_branch_id(
            branch_id, check_existence=False
        )
        if branch_id in branch_ids:
            raise ValueError(f'Branch id "{branch_id}" already exists in the database')
        if parent_branch_id and parent_branch_id not in branch_ids:
            raise ValueError(
                f'Parent branch id "{parent_branch_id}" does not exist in the database'
            )

        self._do_add_branch(branch_id, parent_branch_id)
        self.current_branch_id = branch_id

    def set_branch(self, branch_id):
        """Set the current branch, refusing an id that does not exist.

        Mirrors MATLAB, which calls validate_branch_id here. This was a bare
        assignment behind a "validation logic would go here" placeholder, so a
        typo was accepted and only surfaced later at the next add_docs
        ("Branch '<id>' does not exist.") -- fail-deferred rather than
        fail-silent, but reported at a different point than MATLAB reports it.

        A consequence worth knowing, because it is inherited from MATLAB
        rather than chosen here: an empty id is refused, so the current branch
        cannot be cleared, and since add_branch reads an empty parent as "the
        current branch", a root branch can only be created when there is no
        current branch -- on a fresh database, or after delete_branch drops
        the last one. Neither language can build a multi-root database through
        the API. See VH-Lab/DID-matlab#165.
        """
        branch_id, _ = self._validate_branch_id(branch_id)
        self.current_branch_id = branch_id

    def get_branch(self):
        return self.current_branch_id

    def display_branches(self, branch_id=None):
        """Print the branch hierarchy under a branch, as MATLAB does.

        If BRANCH_ID is empty or not given, the current branch is used.
        Raises if it does not exist.
        """
        if not branch_id:
            branch_id = self.current_branch_id
        branch_id, _ = self._validate_branch_id(branch_id)

        def show(bid, indent):
            print(f"{'  ' * indent} - {bid}")
            for sub_id in self.get_sub_branches(bid):
                show(sub_id, indent + 1)

        show(branch_id, 0)

    # ... other branch-related methods would follow ...

    @abc.abstractmethod
    def _do_get_branch_ids(self):
        pass

    @abc.abstractmethod
    def _do_add_branch(self, branch_id, parent_branch_id):
        pass

    # ... other abstract do_* methods for branches ...

    def all_doc_ids(self):
        return self._do_get_doc_ids()

    def get_doc_ids(self, branch_id=None):
        """Ids of the documents on a branch, defaulting to the current one.

        The guard is ``not branch_id``, not ``is None``: MATLAB's isempty()
        covers both [] and '', and the difference was not cosmetic here. An
        explicit "" used to reach _do_get_doc_ids, whose own guard is
        truthiness, so the branch filter was dropped and the all-branches
        query ran -- get_doc_ids("") returned every document in the database
        rather than the current branch's. See issue #55.
        """
        if not branch_id:
            branch_id = self.current_branch_id
        branch_id, _ = self._validate_branch_id(branch_id)
        return self._do_get_doc_ids(branch_id)

    def validate_docs(self, document_objs):
        """Validate documents against their schemas before they are added.

        Mirrors MATLAB did.database.validate_docs: dependency values are
        checked against the superset of document ids already in the database
        and those in this batch, so a batch may depend on itself.
        """
        from .validate import validate_docs as _validate_docs

        all_ids = [str(i).lower() for i in (self.all_doc_ids() or [])]
        for doc in document_objs:
            doc_props = getattr(doc, "document_properties", doc)
            try:
                all_ids.append(str(doc_props["base"]["id"]).lower())
            except (KeyError, TypeError):
                pass  # ignore this document
        all_ids = sorted(set(all_ids))

        _validate_docs(document_objs, all_ids, debug=self.debug)

    #: Allowed values for the ``OnDuplicate`` option, mirroring DID-matlab's
    #: ``did.database.add_docs`` ``OnDuplicate {mustBeMember(...)} = 'error'``.
    _ON_DUPLICATE_CHOICES = ("ignore", "warn", "error")

    def add_docs(
        self,
        document_objs,
        branch_id=None,
        validate=True,
        OnDuplicate="error",
        custom_file_handler=None,
        **kwargs,
    ):
        """Add documents to a branch.

        ``custom_file_handler`` mirrors MATLAB's ``customFileHandler``
        name-value argument: a callable used to retrieve a file whose location
        is not a local path (``ndic://``, a URL). It is called as
        ``handler(dest_path, source_path)`` and must produce a local file at
        ``dest_path``. DID retrieves no remote file itself; a downstream
        package supplies retrieval through this handler. Only locations marked
        for ingestion are retrieved here, which for remote locations is rare --
        ``ingest`` defaults to 0 for ``url`` and ``ndicloud``. The spelling is
        snake_case to match ``open_doc``'s parameter of the same contract.
        """
        # MATLAB's isempty() covers both [] and '', so "" means the current
        # branch here too. It is deliberately NOT validated: MATLAB's add_docs
        # is the one branch-taking method that does not call
        # validate_branch_id, leaving _do_add_doc's insert to refuse a branch
        # that does not exist -- which it does, and which
        # tests/test_add_docs_atomicity.py pins.
        if not branch_id:
            branch_id = self.current_branch_id

        # Reject invalid OnDuplicate up front (mirror MATLAB mustBeMember) so a
        # typo cannot silently fall through to default behaviour.
        if str(OnDuplicate).lower() not in self._ON_DUPLICATE_CHOICES:
            raise ValueError(
                "OnDuplicate must be one of "
                f"{self._ON_DUPLICATE_CHOICES}; got {OnDuplicate!r}."
            )

        # Ensure all input docs pass schema validation (unless requested not to)
        if validate:
            self.validate_docs(document_objs)

        for doc in document_objs:
            self._do_add_doc(
                doc,
                branch_id,
                OnDuplicate=OnDuplicate,
                custom_file_handler=custom_file_handler,
                **kwargs,
            )

    # ... other document-related methods would follow ...

    @abc.abstractmethod
    def _do_get_doc_ids(self, branch_id=None):
        pass

    @abc.abstractmethod
    def _do_add_doc(self, document_obj, branch_id, **kwargs):
        pass

    def get_docs(self, document_ids, branch_id=None, OnMissing="error", **kwargs):
        is_single = False
        if not isinstance(document_ids, list):
            document_ids = [document_ids]
            is_single = True

        # If branch_id is provided, we might want to validate it or pass it down.
        # Current _do_get_doc doesn't take branch_id, but maybe it should?
        # For now, I'll ignore passing it to _do_get_doc unless I change its signature.
        # But wait, checking if doc is in branch is important if branch_id is given.

        # Checking logic here (inefficient but generic):
        if branch_id is not None:
            branch_doc_ids = self.get_doc_ids(branch_id)
            # If branch doesn't exist? get_doc_ids might return empty or raise?
            # get_doc_ids calls _do_get_doc_ids.

        docs = []
        for doc_id in document_ids:
            if branch_id is not None and doc_id not in branch_doc_ids:
                # Document not in branch
                if OnMissing == "error":
                    raise ValueError(
                        f"Document {doc_id} not found in branch {branch_id}"
                    )
                elif OnMissing == "warn":
                    print(f"Warning: Document {doc_id} not found in branch {branch_id}")
                    continue
                else:
                    continue

            docs.append(self._do_get_doc(doc_id, OnMissing=OnMissing, **kwargs))

        if not docs and OnMissing != "ignore" and len(document_ids) > 0:
            # If filtered out all?
            pass

        if is_single:
            return docs[0] if docs else None
        else:
            return docs

    @abc.abstractmethod
    def _do_get_doc(self, document_id, OnMissing="error", **kwargs):
        pass

    def remove_docs(self, document_ids, branch_id=None, **kwargs):
        if not isinstance(document_ids, list):
            document_ids = [document_ids]

        if not branch_id:
            branch_id = self.current_branch_id
        branch_id, _ = self._validate_branch_id(branch_id)

        for doc_id in document_ids:
            self._do_remove_doc(doc_id, branch_id, **kwargs)

    @abc.abstractmethod
    def _do_remove_doc(self, document_id, branch_id, **kwargs):
        pass

    def _validate_branch_id(self, branch_id, check_existence=True):
        """Mirror of MATLAB's validate_branch_id.

        Returns (branch_id, branch_ids) so a caller that needs the full list -
        delete_branch, to pick the next current branch - does not query twice.
        """
        if not isinstance(branch_id, str) or not branch_id:
            raise ValueError("Branch ID must be a non-empty string")
        branch_ids = self.all_branch_ids()
        if check_existence and branch_id not in branch_ids:
            raise ValueError(f'Branch ID "{branch_id}" does not exist in the database')
        return branch_id, branch_ids

    def freeze_branch(self, branch_id=None):
        """Mark a branch as protected from modification.

        Mirrors MATLAB's freeze_branch, including its scope: frozen_branch_ids
        is an in-memory property that is never written to the database, so a
        freeze lasts only for this object's lifetime and is invisible to any
        other process or to MATLAB. It is enforced in exactly one place,
        delete_branch, in both languages.
        """
        if not branch_id:
            branch_id = self.current_branch_id
        branch_id, _ = self._validate_branch_id(branch_id)
        if branch_id not in self.frozen_branch_ids:
            self.frozen_branch_ids.append(branch_id)

    def is_branch_editable(self, branch_id=None):
        """True if the branch is neither frozen nor a parent of another branch.

        Mirrors MATLAB's is_branch_editable: the two conditions delete_branch
        refuses on, made available to ask about beforehand.
        """
        if not branch_id:
            branch_id = self.current_branch_id
        branch_id, _ = self._validate_branch_id(branch_id)
        return branch_id not in self.frozen_branch_ids and not self.get_sub_branches(
            branch_id
        )

    def delete_branch(self, branch_id=None):
        """Delete a branch, refusing the cases MATLAB refuses.

        Previously this was a bare delegate with a "validation logic would go
        here" placeholder, so all three of MATLAB's guards were missing:
        deleting a branch that does not exist was a silent no-op, deleting a
        parent branch surfaced as a raw sqlite3.IntegrityError from the
        FOREIGN KEY rather than a described error, and a frozen branch was not
        refused at all. It also left current_branch_id naming a branch that no
        longer exists, so the next add_branch inherited a deleted parent and
        failed on the same FOREIGN KEY.

        If BRANCH_ID is empty or not given, the current branch is used.
        """
        if not branch_id:
            branch_id = self.current_branch_id
        branch_id, branch_ids = self._validate_branch_id(branch_id)

        if branch_id in self.frozen_branch_ids:
            raise ValueError(f'Branch id "{branch_id}" is frozen and cannot be deleted')
        if self.get_sub_branches(branch_id):
            raise ValueError(
                f'Branch id "{branch_id}" has sub-branches and cannot be deleted'
            )

        self._do_delete_branch(branch_id)

        # MATLAB drops branch_id from frozen_branch_ids here. That is
        # unreachable in both languages -- the frozen guard above has already
        # refused -- so it is not mirrored.

        # If the deleted branch was the current one, fall back as MATLAB does:
        # to the first of the branch ids read before the delete, or to none if
        # that was the branch just deleted. Both languages read those ids with
        # the same unordered `SELECT DISTINCT branch_id FROM branches`, so the
        # fallback picks the same branch in both.
        if self.current_branch_id == branch_id:
            self.current_branch_id = branch_ids[0] if branch_ids else ""
            if self.current_branch_id == branch_id:
                self.current_branch_id = ""

    @abc.abstractmethod
    def _do_delete_branch(self, branch_id):
        pass

    def get_sub_branches(self, branch_id=None):
        if not branch_id:
            branch_id = self.current_branch_id
        branch_id, _ = self._validate_branch_id(branch_id)
        return self._do_get_sub_branches(branch_id)

    @abc.abstractmethod
    def _do_get_sub_branches(self, branch_id):
        pass

    def get_branch_parent(self, branch_id=None):
        if not branch_id:
            branch_id = self.current_branch_id
        branch_id, _ = self._validate_branch_id(branch_id)
        return self._do_get_branch_parent(branch_id)

    @abc.abstractmethod
    def _do_get_branch_parent(self, branch_id):
        pass

    def close_doc(self, file_obj):
        """Close a file object previously returned by open_doc.

        MATLAB's close_doc delegates to do_close_doc, whose default
        implementation is a single file_obj.fclose(); there is no separate
        _do_close_doc here because no implementation overrides it. Python's
        file objects also close on garbage collection, so this is the explicit
        call rather than a leak fix.
        """
        if file_obj is not None:
            file_obj.fclose()

    def get_preference_names(self):
        """Names of every preference set on this object.

        MATLAB returns a cell array; this returns a list. Empty when none
        have been set.
        """
        return list(self.preferences.keys())

    def get_preference(self, pref_name, *default_value):
        """Value of a preference, or DEFAULT_VALUE if it was never set.

        Mirrors MATLAB's three-argument form: with no default given, an unset
        preference raises rather than returning None -- which is why the
        default is *args rather than default=None. None is a legitimate stored
        value, and the two cases have to stay distinguishable.
        """
        if not pref_name or not isinstance(pref_name, str):
            raise ValueError("get_preference requires a valid preference name")
        if len(default_value) > 1:
            raise TypeError(
                f"get_preference takes at most one default value, "
                f"got {len(default_value)}"
            )
        try:
            return self.preferences[pref_name]
        except KeyError:
            if default_value:
                return default_value[0]
            raise ValueError(f'Preference value "{pref_name}" is not defined') from None

    def set_preference(self, pref_name, value=None):
        """Set a preference. An omitted VALUE stores None, as MATLAB stores []."""
        if not pref_name or not isinstance(pref_name, str):
            raise ValueError("set_preference requires a valid preference name")
        self.preferences[pref_name] = value

    def search(self, query_obj, branch_id=None):
        from .datastructures import field_search

        if not branch_id:
            branch_id = self.current_branch_id
        branch_id, _ = self._validate_branch_id(branch_id)

        doc_ids = self.get_doc_ids(branch_id)
        docs = self.get_docs(doc_ids, OnMissing="ignore")
        if docs is None:
            docs = []
        if not isinstance(docs, list):
            docs = [docs]

        search_params = query_obj.to_search_structure()

        matched_ids = []
        for doc in docs:
            if doc and field_search(doc.document_properties, search_params):
                matched_ids.append(doc.id())

        return matched_ids

    # ... other abstract do_* methods for documents ...

    @abc.abstractmethod
    def do_run_sql_query(self, query_str, **kwargs):
        pass
