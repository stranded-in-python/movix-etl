from typing import Optional

from pydantic import BaseSettings, Field


class Fields(BaseSettings):
    start_pivot: Optional[str]      = Field(
        default=None,
        env='START_PIVOT_KEY',
    )
    end_pivot: Optional[str]        = Field(
        default=None,
        env='END_PIVOT_KEY',
    )
    loaded_data: Optional[str]      = Field(
        default=None,
        env='LOADED_DATA_KEY',
    )
    current_table: Optional[str]    = Field(
        default=None,
        env='CURRENT_TABLE_KEY',
    )
    data_is_loaded: bool            = Field(
        default=True,
        env='DATA_IS_LOADED',
    )
    index_is_created: bool          = Field(
        default=False,
        env='INDEX_IS_CREATED',
    )

    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'
        arbitrary_types_allowed = True

    def get(self, key: str, default=None):
        return getattr(self, key, default)
