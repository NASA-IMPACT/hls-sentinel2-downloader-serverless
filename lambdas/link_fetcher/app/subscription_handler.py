import logging
import os

from mangum import Mangum

from app.subscription_endpoint import (
    EndpointConfig,
    build_app,
)

logging.getLogger("app").setLevel(logging.INFO)

config = EndpointConfig.load_from_secrets_manager(os.environ["STAGE"])
app = build_app(config)
handler = Mangum(app)

print("BUILD THE LAMBDA HANDLER")
