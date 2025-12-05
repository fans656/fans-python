import fire
from fans.logger import get_logger


logger = get_logger(__name__)


class CLI:

    def info(self):
        """
        Show info about nos
        """
        print('info')

    def serve(self, host: str = '127.0.0.1', port: int = 8000, conf: str = ''):
        """
        Run nos server
        """
        import uvicorn
        from .app import app
        from .service import Service
        if conf:
            service = Service.get_instance()
            service.setup(conf)
            return
        else:
            logger.warning('no conf specified')
        uvicorn.run(app, host=host, port=port)


if __name__ == '__main__':
    fire.Fire(CLI(), name='nos')
