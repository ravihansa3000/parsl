import tornado.ioloop
import tornado.web
import json
from parsl.monitoring.db_logger import get_db_logger


class MainHandler(tornado.web.RequestHandler):
    """ A handler for all requests/logs sent to the logging server."""
    def initialize(self, db_logger_config):
        """This function is called on every request which is not ideal but __init__ does not appear to work."""
        self.db_logger_config = db_logger_config

    def get(self):
        """Defines responses to get requests for the / ending. Not used by Parsl but could be."""
        self.write('Hello world - Parsl Logging Server')
        self.flush()

    def post(self):
        """
        Defines responses to post requests for the / ending. Receives logs from workers and main dfk in the body of the post request.
        Should be log=json.dumps(info). Then writes this info to the database using the database handler.
        This needs to be a quick function so that the server can accept other requests and may be a bottle neck for incoming logs.
        """
        arg = json.loads(self.get_body_argument('log'))
        try:
            self.application.logger.info('from tornado task ' + str(arg.get('task_id', 'NO TASK')), extra=arg)
        except AttributeError as e:
            self.application.logger = get_db_logger(logger_name='loggingserver', is_logging_server=True, **self.db_logger_config)
            self.application.logger.info('from tornado task ' + str(arg.get('task_id', 'NO TASK')), extra=arg)


def run(db_logger_config):
    """ Set up the logging server according to configurations the user specified. This is the function launched as a separate process from the DFK in order to start logging. """
    # Assumtion that db_logger_config is not none because if it were this server should not have been started
    app = tornado.web.Application([(r"/", MainHandler, dict(db_logger_config=db_logger_config))])
    app.listen(db_logger_config.get('web_app_port', 8899))
    tornado.ioloop.IOLoop.current().start()


if __name__ == "__main__":
    run()
