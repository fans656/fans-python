from fans.logger import get_logger
from fans.jober import Jober


logger = get_logger(__name__)


class CLI:
    
    def serve(self, conf_path: str = None, host: str = '127.0.0.1', port: int = 8000, token: str = None):
        """Run in server mode"""
        import logging

        import uvicorn

        from .app import app

        logging.root.setLevel(logging.INFO)

        jober = Jober(conf_path)
        Jober.set_instance(jober)
        
        if token:
            from fastapi import Request
            from fastapi.responses import JSONResponse

            @app.middleware('http')
            async def verify_token(request: Request, callnext):
                if request.cookies.get('token') != token:
                    return JSONResponse(content={'error': 'invalid token'}, status_code=401)
                return await callnext(request)

            logger.info(f'using token {token}')

        uvicorn.run(app, host=host, port=port)


if __name__ == '__main__':
    import fire
    fire.Fire(CLI(), name='jober')
