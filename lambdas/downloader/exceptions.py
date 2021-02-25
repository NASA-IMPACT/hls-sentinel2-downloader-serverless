class ChecksumRetrievalException(Exception):
    pass


class GranuleNotFoundException(Exception):
    pass


class RetryLimitReachedException(Exception):
    pass


class SciHubAuthenticationNotRetrievedException(Exception):
    pass


class FailedToHandleInvalidFileException(Exception):
    pass


class FailedToHandleValidFileException(Exception):
    pass


class FailedToDownloadFileException(Exception):
    pass


class FailedToUpdateGranuleDownloadStartException(Exception):
    pass
