from typing import Any

from cassio.table.table_types import rearrange_pk_type

from .base_table import BaseTableMixin


class TypeNormalizerMixin(BaseTableMixin):
    clustered: bool = False
    elastic: bool = False

    def __init__(self, *pargs: Any, **kwargs: Any) -> None:
        if "primary_key_type" in kwargs:
            if "row_id_type" in kwargs:
                raise ValueError(
                    "Use of 'row_id_type' not allowed with 'primary_key_type'. "
                    "Please pass 'partition_id_type' instead of 'primary_key_type', "
                    "or specify 'num_partition_keys' to cut the primary key."
                )
            pk_arg = kwargs["primary_key_type"]
            num_elastic_keys = len(kwargs["keys"]) if self.elastic else None
            num_partition_keys = kwargs.get("num_partition_keys")
            col_type_map = rearrange_pk_type(
                pk_type=pk_arg,
                clustered=self.clustered,
                num_partition_keys=num_partition_keys,
                num_elastic_keys=num_elastic_keys,
            )
            new_kwargs = {
                **col_type_map,
                **{
                    k: v
                    for k, v in kwargs.items()
                    if k not in {"num_partition_keys", "primary_key_type"}
                },
            }
        else:
            new_kwargs = kwargs
        super().__init__(*pargs, **new_kwargs)
