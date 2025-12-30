import azure.functions as func
from batch_cleaner import run_batch_clean
app = func.FunctionApp()

@app.function_name(name="phase1_clean_batch")
@app.route(route="phase1_clean_batch", auth_level=func.AuthLevel.FUNCTION)
def phase1_clean_batch(req: func.HttpRequest) -> func.HttpResponse:
    return run_batch_clean(req)
