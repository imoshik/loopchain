from . import BlockHeader
from .. import v0_1a


class BlockProver(v0_1a.BlockProver):
    version = BlockHeader.version
