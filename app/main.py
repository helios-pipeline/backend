import logging
import os
import clickhouse_connect
from dotenv import load_dotenv
from flask import Flask, send_from_directory
from flask_cors import CORS
from app.api.routes import api
from flask import g


def create_app(config=None, client=None):
    print(f"Creating app with config: {config}")
    app = Flask(__name__, static_folder='../dist')
    CORS(app)
    
    @app.route('/', defaults={'path': ''})
    @app.route('/<path:path>')
    def serve(path):
        if path != "" and os.path.exists(app.static_folder + '/' + path):
            return send_from_directory(app.static_folder, path)
        else:
            return send_from_directory(app.static_folder, 'index.html')

    load_dotenv()

    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger(__name__)
    
    app.config["CH_HOST"] = os.getenv("CH_HOST", "localhost")
    app.config["CH_PORT"] = int(os.getenv("CH_PORT", 8123))
    app.config["CH_USER"] = os.getenv("CH_USER", "default")
    app.config["CH_PASSWORD"] = os.getenv("CH_PASSWORD", "")
    app.config["CHAT_GPT_API_KEY"] = os.getenv("CHAT_GPT_API_KEY", "")

    if config:
        logger.debug(f"Updating config: {config}")
        app.config.update(config)



    app.register_blueprint(api, url_prefix='/api')

    def get_ch_client():
        if 'ch_client' not in g:
            if client:
                g.ch_client = client
            else:
                logger.debug("Creating new ClickHouse client")
                try:
                    g.ch_client = clickhouse_connect.get_client(
                        host=app.config["CH_HOST"],
                        port=app.config["CH_PORT"],
                        username=app.config["CH_USER"],
                        password=app.config["CH_PASSWORD"],
                    )
                    logger.debug("ClickHouse client created successfully")
                except Exception as e:
                    logger.error(f"Failed to create ClickHouse client: {str(e)}")
                    raise
        return g.ch_client

    @app.teardown_appcontext
    def close_ch_client(e=None):
        ch_client = g.pop('ch_client', None)
        if ch_client is not None and ch_client != client:
            ch_client.close()
    
    app.get_ch_client = get_ch_client

    return app


if __name__ == "__main__":
    app = create_app()
    app.run()
