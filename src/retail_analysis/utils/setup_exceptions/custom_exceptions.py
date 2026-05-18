"""
Pipeline-specific exceptions.

Each exception maps to a distinct failure domain, making it easy to
catch and handle errors at the appropriate level in the orchestrator.
"""


class PipelineError(Exception):
    """Base class for all pipeline-related exceptions."""
    pass


class ConfigError(PipelineError):
    """Raised when pipeline configuration is missing or invalid."""
    pass

class DataError(PipelineError):
    """Base class for data-related errors."""
    pass

class FileValidationError(DataError):
    """Raised when expected source files are missing or corrupt."""
    pass


class ReadError(DataError):
    """Raised when reading  source data fails."""
    pass


class TransformError(DataError):
    """Raised when a business-logic transformation fails."""
    pass


class DataQualityError(DataError):
    """Raised when data quality checks detect critical violations."""
    pass


class WriteError(DataError):
    """Raised when persisting a DataFrame to Delta/storage fails."""
    pass

class ReportingError(PipelineError):
    """Base class for all reporting/KPI-related errors."""
    pass


class KPIQueryError(ReportingError):
    """Raised when SQL queries for KPIs fail."""
    pass
