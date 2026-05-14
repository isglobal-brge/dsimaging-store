import datetime as dt
import importlib.util
import io
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "store_controller", ROOT / "controller" / "controller.py"
)
controller = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(controller)


class FakeBody(io.BytesIO):
    pass


class FakePaginator:
    def __init__(self, objects):
        self.objects = objects

    def paginate(self, Bucket, Prefix, Delimiter=None):
        contents = []
        common_prefixes = set()
        for key, value in sorted(self.objects.items()):
            if not key.startswith(Prefix):
                continue
            if Delimiter:
                rest = key[len(Prefix):]
                if Delimiter in rest:
                    common_prefixes.add(Prefix + rest.split(Delimiter, 1)[0] + Delimiter)
                    continue
            contents.append({
                "Key": key,
                "Size": len(value),
                "LastModified": dt.datetime(2026, 5, 13, tzinfo=dt.timezone.utc),
                "ETag": '"fake-etag"',
            })
        page = {}
        if contents:
            page["Contents"] = contents
        if common_prefixes:
            page["CommonPrefixes"] = [
                {"Prefix": prefix} for prefix in sorted(common_prefixes)
            ]
        yield page


class FakeS3:
    def __init__(self, objects):
        self.objects = dict(objects)

    def get_paginator(self, name):
        if name != "list_objects_v2":
            raise ValueError(name)
        return FakePaginator(self.objects)

    def head_object(self, Bucket, Key):
        if Key not in self.objects:
            raise KeyError(Key)
        return {
            "ContentLength": len(self.objects[Key]),
            "LastModified": dt.datetime(2026, 5, 13, tzinfo=dt.timezone.utc),
            "ETag": '"fake-etag"',
        }

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise KeyError(Key)
        return {"Body": FakeBody(self.objects[Key])}

    def upload_file(self, Filename, Bucket, Key):
        self.objects[Key] = Path(Filename).read_bytes()

    def delete_objects(self, Bucket, Delete):
        for item in Delete.get("Objects", []):
            self.objects.pop(item["Key"], None)
        return {}


class ReconcileTests(unittest.TestCase):
    def setUp(self):
        self.bucket = "imaging-data"
        self.old_bucket = controller.BUCKET
        self.old_get_s3 = controller.get_s3
        controller.BUCKET = self.bucket

    def tearDown(self):
        controller.BUCKET = self.old_bucket
        controller.get_s3 = self.old_get_s3

    def test_reconcile_removes_managed_artifacts_when_images_disappear(self):
        prefix = "datasets/study_ct_v1"
        artifacts = [
            "manifest.yaml",
            "indexes/content_hash_index.parquet",
            "indexes/masks_content_hash_index.parquet",
            "metadata/sample_manifests.parquet",
            "metadata/samples.parquet",
        ]
        s3 = FakeS3({f"{prefix}/{suffix}": b"stale" for suffix in artifacts})
        controller.get_s3 = lambda: s3

        n_samples, n_masks = controller.reconcile_dataset("study_ct_v1")

        self.assertEqual((n_samples, n_masks), (0, 0))
        for suffix in artifacts:
            self.assertNotIn(f"{prefix}/{suffix}", s3.objects)

    def test_reconcile_removes_stale_mask_index_when_masks_disappear(self):
        prefix = "datasets/study_ct_v1"
        stale_mask_index = f"{prefix}/indexes/masks_content_hash_index.parquet"
        s3 = FakeS3({
            f"{prefix}/source/images/case001.nii.gz": b"image",
            stale_mask_index: b"stale",
        })
        controller.get_s3 = lambda: s3

        n_samples, n_masks = controller.reconcile_dataset("study_ct_v1")

        self.assertEqual((n_samples, n_masks), (1, 0))
        self.assertIn(f"{prefix}/manifest.yaml", s3.objects)
        self.assertIn(f"{prefix}/indexes/content_hash_index.parquet", s3.objects)
        self.assertNotIn(stale_mask_index, s3.objects)


if __name__ == "__main__":
    unittest.main()
