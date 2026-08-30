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
        branch: on a fresh database, or after set_branch("") or deleting the
        last branch. MATLAB has no other way to make one and neither has this.
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
        # Validation logic would go here
        self.current_branch_id = branch_id

    def get_branch(self):
        return self.current_branch_id

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
        if branch_id is None:
            branch_id = self.current_branch_id
        # Validation logic would go here
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
        if branch_id is None:
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

        if branch_id is None:
            branch_id = self.current_branch_id

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
        if branch_id is None:
            branch_id = self.current_branch_id
        return self._do_get_sub_branches(branch_id)

    @abc.abstractmethod
    def _do_get_sub_branches(self, branch_id):
        pass

    def get_branch_parent(self, branch_id=None):
        if branch_id is None:
            branch_id = self.current_branch_id
        return self._do_get_branch_parent(branch_id)

    @abc.abstractmethod
    def _do_get_branch_parent(self, branch_id):
        pass

    def search(self, query_obj, branch_id=None):
        from .datastructures import field_search

        if branch_id is None:
            branch_id = self.current_branch_id

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
