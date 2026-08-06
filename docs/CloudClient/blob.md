# Blob interaction with `cfa.cloudops.CloudClient`

Another benefit to the `CloudClient` class is its Blob Storage interaction. There are several functions for uploading to and downloading from Blob Storage,

- create_blob_container
- upload_files
- upload_folders
- download_file
- download_folder
- list_blob_files
- delete_blob_file
- delete_blob_folder
- download_after_job
- update_blob_protection

## Asynchronous Folder Downloads

For large folder downloads, use `async_download_folder` to improve throughput with concurrent blob transfers.

Key arguments:

- `src_path`: folder path in blob storage
- `dest_path`: local destination directory
- `container_name`: source container
- `include_extensions` / `exclude_extensions`: optional extension filters
- `max_concurrent_downloads`: max concurrent downloads (default 20)

### Example

```python
client = CloudClient()
client.async_download_folder(
	src_path="job-123/outputs",
	dest_path="./outputs",
	container_name="output-test",
	include_extensions=[".csv", ".json"],
	max_concurrent_downloads=30,
)
```

## Asynchronous Folder Uploads

For large uploads, use `async_upload_folder` to upload folders concurrently.

Key arguments:

- `folders`: a folder path string or a list of folder paths
- `container_name`: destination container
- `location_in_blob`: destination prefix within the container
- `include_extensions` / `exclude_extensions`: optional extension filters
- `max_concurrent_uploads`: max concurrent uploads (default 20)
- `legal_hold`, `immutability_lock_days`, `read_only`: optional blob protection settings

### Example

```python
client = CloudClient()
client.async_upload_folder(
	folders=["./results", "./logs"],
	container_name="output-test",
	location_in_blob="batch-runs/2026-07-23",
	exclude_extensions=[".tmp"],
	max_concurrent_uploads=25,
)
```
