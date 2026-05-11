"""
Pipeline-specific exceptions.

Each exception maps to a distinct failure domain, making it easy to
catch and handle errors at the appropriate level in the orchestrator.
"""


class ConfigError(Exception):
    """Raised when pipeline configuration is missing or invalid."""
    pass


class FileValidationError(Exception):
    """Raised when expected source files are missing or corrupt."""
    pass


class IngestionError(Exception):
    """Raised when reading  source data fails."""
    pass


class TransformError(Exception):
    """Raised when a business-logic transformation fails."""
    pass


class DataQualityError(Exception):
    """Raised when data quality checks detect critical violations."""
    pass


class WriteError(Exception):
    """Raised when persisting a DataFrame to Delta/storage fails."""
    pass