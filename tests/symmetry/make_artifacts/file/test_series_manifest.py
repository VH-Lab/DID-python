"""makeArtifact symmetry test: build file series manifests both languages must read.

Mirrors DID-matlab's
tests_symmetry/+did/+symmetry/+makeArtifacts/+file/seriesManifest.m.

This is the SECOND shared binary format after ``.fileCacheInfo``. A series
member has no ``file_info`` entry and no files-table row: the manifest is
the only record of its uid, so a port that reads these bytes even slightly
differently resolves members to the wrong file or to none -- and, because a
uid names a cache slot, the wrong file is the failure that no later read can
detect.

Not one manifest but five, matching the MATLAB test one-for-one, because
the parts of this format that are easy to get wrong are not exercised by an
ordinary one:

- dense    - the baseline. Header fields, and member i at
             32 + i*uid_width with no name section (flags bit 0 clear).
- sparse   - absent members as all-NUL uid records. Gaps at the first slot,
             an interior pair and nothing at the end.
- named    - empty-name edges: slot 1 empty (first), slots 3-4 empty
             (interior run), slot 6 empty (last), so ``lastOffset ==
             firstOffset`` is exercised for each case.
- allEmpty - the name section present but every name empty. Separates
             "no name section" from "a section of nothing".
- wideUid  - uid_width 40, with uids that actually use all 40. It is a
             HEADER FIELD, not the constant 33 that did.ido.unique_id
             happens to produce.

Slots are ONE-BASED in this file, as they are throughout the Python API,
and ZERO-BASED on disk. ``manifest.json`` states everything in the file's
own zero-based terms so a reader never has to guess which convention a
number is in.
"""

import json
import os
import shutil

from did.file import (
    read_series_manifest,
    read_series_manifest_uid,
    write_series_manifest,
)
from tests.symmetry.conftest import PYTHON_ARTIFACTS

ARTIFACT_DIR = os.path.join(
    PYTHON_ARTIFACTS, "file", "seriesManifest", "testSeriesManifestArtifacts"
)


def uid_of(index, width=33):
    """A deterministic uid exactly ``width`` characters wide.

    Digits only, safe as a basename under :func:`is_safe_uid` and identical
    in both languages without any encoding question.
    """
    return f"{index:0{width}d}"


ARTIFACT_NAMES = ["dense", "sparse", "named", "allEmpty", "wideUid"]


def _spec():
    def uid(i, w=33):
        return uid_of(i, w)

    return {
        # Four members, no name section.
        "dense": {
            "uids": [uid(1), uid(2), uid(3), uid(4)],
            "names": None,
            "uid_width": 33,
        },
        # Six slots: members present at 2, 5, 6. Gaps at 1, 3-4, and none at end.
        "sparse": {
            "uids": ["", uid(12), "", "", uid(15), uid(16)],
            "names": None,
            "uid_width": 33,
        },
        # Empty-name edges: 1, 3-4, 6 empty; 2 and 5 keep a subfolder because
        # a bare basename would lose which pyramid level the chunk came from.
        "named": {
            "uids": [uid(21), uid(22), uid(23), uid(24), uid(25), uid(26)],
            "names": ["", "0/0.1.2.3", "", "", "1/4.5.6.7", ""],
            "uid_width": 33,
        },
        # A name section of nothing.
        "allEmpty": {
            "uids": [uid(31), uid(32), uid(33)],
            "names": ["", "", ""],
            "uid_width": 33,
        },
        # uid_width 40, fully used.
        "wideUid": {
            "uids": [uid(41, 40), uid(42, 40), uid(43, 40)],
            "names": None,
            "uid_width": 40,
        },
    }


class TestSeriesManifestArtifacts:
    def test_series_manifest_artifacts(self):
        if os.path.isdir(ARTIFACT_DIR):
            shutil.rmtree(ARTIFACT_DIR)
        os.makedirs(ARTIFACT_DIR)

        spec = _spec()
        files = []
        for name in ARTIFACT_NAMES:
            this = spec[name]
            file_name = f"{name}.manifest"
            full_path = os.path.join(ARTIFACT_DIR, file_name)

            if this["names"] is None:
                write_series_manifest(
                    full_path, this["uids"], uid_width=this["uid_width"]
                )
            else:
                write_series_manifest(
                    full_path,
                    this["uids"],
                    source_names=this["names"],
                    uid_width=this["uid_width"],
                )

            files.append(
                {
                    "name": name,
                    "file": file_name,
                    "count": len(this["uids"]),
                    "uidWidth": this["uid_width"],
                    "hasSourceNames": this["names"] is not None,
                    # Stated in the file's own ZERO-BASED terms: entry j of
                    # this array is member j on disk.
                    "uids": this["uids"],
                    "sourceNames": this["names"] if this["names"] is not None else [],
                }
            )

        manifest = {
            "magic": "DIDFSER1",
            "formatVersion": 1,
            "indexBase": "zero",
            "files": files,
        }
        with open(os.path.join(ARTIFACT_DIR, "manifest.json"), "w") as f:
            json.dump(manifest, f)

        # Self-check: confirm what is claimed before any cross-language
        # claim rests on it, exactly as the fileCache artifact does.
        for name in ARTIFACT_NAMES:
            this = spec[name]
            full_path = os.path.join(ARTIFACT_DIR, f"{name}.manifest")
            m = read_series_manifest(full_path)
            assert m["count"] == len(this["uids"]), name
            assert m["uid_width"] == this["uid_width"], name
            assert m["uids"] == this["uids"], name
            assert m["has_source_names"] == (this["names"] is not None), name
            if this["names"] is None:
                assert m["source_names"] == [], name
            else:
                assert m["source_names"] == this["names"], name

            # O(1) slot read must agree with the whole-file read for every
            # slot. That path is the reason this format was chosen over
            # inline file_info; a port that has it disagree has given away
            # the property while still passing a whole-file read.
            for slot in range(1, len(this["uids"]) + 1):
                u, c = read_series_manifest_uid(full_path, slot)
                assert u == this["uids"][slot - 1], f"{name} slot {slot}"
                assert c == len(this["uids"]), name

            # One past the end is a miss, not an error.
            u, _c = read_series_manifest_uid(full_path, len(this["uids"]) + 1)
            assert u == "", name
