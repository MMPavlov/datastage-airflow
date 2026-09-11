class AirflowException(Exception):
    pass


class AirflowFailException(AirflowException):
    pass


class AirflowSkipException(AirflowException):
    pass


class AirflowSensorTimeout(AirflowException):
    pass
