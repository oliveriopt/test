def hello_world(request):
    return "Hello from Cloud Functions!"



gcloud functions deploy hello_world \
  --runtime python310 \
  --trigger-http \
  --allow-unauthenticated \
  --entry-point hello_world
