class Error(Exception):
    pass


class AppConfigValidationError(Error):
    def __str__(self):
        msg = super().__str__()
        return f'Application config validation failed. {msg}'


class RankConfigValidationError(Error):
    def __str__(self):
        msg = super().__str__()
        return f'Rank config validation failed. {msg}'
