import json, re
from google.cloud import vision, storage
from google.oauth2 import service_account

mime_type = "application/pdf"
key = "AIzaSyCdflOSDTb4_l6M7cxIWstXMB6Gc6owirI"
credentials = service_account.Credentials.from_service_account_file(
    "my-happy-valley-72f3944fa80a.json")

def async_detect_document(gcs_source_uri, gcs_destination_uri):
    batch_size = 100
    client = vision.ImageAnnotatorClient(credentials=credentials)
    feature = vision.Feature(type_=vision.Feature.Type.DOCUMENT_TEXT_DETECTION)

    gcs_source = vision.GcsSource(uri=gcs_source_uri)
    input_config = vision.InputConfig(
        gcs_source=gcs_source, mime_type=mime_type)

    gcs_destination = vision.GcsDestination(uri=gcs_destination_uri)
    output_config = vision.OutputConfig(
        gcs_destination=gcs_destination, batch_size=batch_size)

    async_request = vision.AsyncAnnotateFileRequest(
        features=[feature], input_config=input_config,
        output_config=output_config)

    operation = client.async_batch_annotate_files(
        requests=[async_request])

    print("Waiting for the operation to finish.")
    operation.result(timeout=420)


def write_to_text(gcs_destination_uri):
    storage_client = storage.Client(credentials=credentials)
    match = re.match(r"gs://([^/]+)/(.+)", gcs_destination_uri)
    bucket_name = match.group(1)
    prefix = match.group(2)

    bucket = storage_client.get_bucket(bucket_name)

    blob_list = list(bucket.list_blobs(prefix=prefix))
    print("Output files:")
    for blob in blob_list:
        print(blob.name)

    for i in range(len(blob_list)):
        output = blob_list[i]
        json_string = output.download_as_string()
        response = json.loads(json_string)

        file = open(f"batch_{i}.txt", "w")

        for j in range(len(response["responses"])):
            first_page_response = response["responses"][j]
            annotation = first_page_response["fullTextAnnotation"]

            print("Full text:\n")
            print(annotation["text"])
            file.write(annotation["text"])


source = r"gs://gcp_ocr_test_cx/Quantcraft_ ESG meets NLP_ systematic ESG investing.pdf"
# source = r"gs://gcp_ocr_test_cx/cv_cx.aws.2021Feb.pdf"
destination = r"gs://gcp_ocr_test_cx/ocr_result"

async_detect_document(source, destination)
write_to_text(destination)