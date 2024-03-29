class ChecksumRetrievalException(Exception):
    pass


class GranuleNotFoundException(Exception):
    pass


class GranuleAlreadyDownloadedException(Exception):
    pass


class RetryLimitReachedException(Exception):
    pass


class CopernicusAuthenticationNotRetrievedException(Exception):
    pass


class CopernicusTokenNotRetrievedException(Exception):
    pass


class FailedToDownloadFileException(Exception):
    pass


class FailedToRetrieveGranuleException(Exception):
    pass


class FailedToUploadFileException(Exception):
    pass


class FailedToUpdateGranuleDownloadFinishException(Exception):
    pass
