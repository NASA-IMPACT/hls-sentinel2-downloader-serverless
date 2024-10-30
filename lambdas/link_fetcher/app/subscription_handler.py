import os

from mangum import Mangum
from app.subscription_endpoint import (
    EndpointConfig,
    build_app,
)


config = EndpointConfig.load_from_secrets_manager(os.environ["STAGE"])
app = build_app(config)
handler = Mangum(app)
