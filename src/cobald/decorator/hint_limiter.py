import asyncio
import time
import logging
from functools import partial

from cobald.interfaces import Pool, PoolDecorator

from cobald.daemon import service


@service(flavour=asyncio)
class HintLimiter(PoolDecorator):
    """
    Limits for changes to the demand of a pool

    :param target: the pool on which changes are modified
    :param period: time between updates before we go into "drain mode"
    :param port: port to listen on

    Starts a TCP listener that receives "demand hints". When receiving a number,
    sets the maximum ``target.demand'' to ``target.supply + hint''. This number will be constant until
    another hint is received.

    If no hints are received for longer than ``period'', will enter a "drain mode".
    In this mode the ``target.demand'' can never be higher than ``target.supply''.
    """

    @property
    def demand(self) -> float:
        return self.target.demand

    @demand.setter
    def demand(self, value: float):
        self._check_freshness()
        self.target.demand = self._clamp_demand(value)

    def set_max(self):
        self.maximum = self.target.supply + self.hint

    def _check_freshness(self):
        if time.time() - self._updated > self.period:
            self.hint = 0
            self.set_max()

    def _clamp_demand(self, value):
        demand = max(0, min(self.maximum, value))
        return type(value)(demand)

    def __init__(
        self,
        target: Pool,
        period: float,
        port: int
    ):
        super().__init__(target)
        self.hint = 0
        self.period = period
        self._updated = time.time()
        self._port = port
        self.set_max()
        self._logger = logging.getLogger(self.__class__.__name__)

    async def run(self):
        server = await asyncio.start_server(
            partial(_handle_hint, self), '127.0.0.1', self._port)
        async with server:
            await server.serve_forever()


async def _handle_hint(slf, reader, writer):
    hint = await reader.read()
    try:
        slf.hint = float(hint.decode())
    except ValueError:
        slf.hint = 0
        slf._logger.warning("Received invalid demand hint: %s", hint.decode())
    else:
        slf._updated = time.time()
        slf._logger.debug("Received demand hint %f", slf.hint)
    slf.set_max()
