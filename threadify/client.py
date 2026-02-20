from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import fields
from typing import Any

import websockets

from threadify.connection import Connection
from threadify.models import (
    ACTION_CONNECT,
    FIELD_ACTION,
    FIELD_API_KEY,
    FIELD_MAX_IN_FLIGHT,
    FIELD_MESSAGE,
    FIELD_SERVICE_NAME,
    FIELD_STATUS,
    STATUS_SUCCESS,
    ConnectOptions,
    require_non_empty,
)


def _copy_connect_options(src: ConnectOptions) -> ConnectOptions:
    data = {f.name: getattr(src, f.name) for f in fields(ConnectOptions)}
    return ConnectOptions(**data)


def _build_connect_options(
    *,
    base: ConnectOptions | None,
    service_name: str | None,
    ws_url: str | None,
    graphql_url: str | None,
    debug: bool | None,
    max_in_flight: int | None,
    connect_timeout: float | None,
    logger: logging.Logger | None = None,
) -> ConnectOptions:
    cfg = _copy_connect_options(base) if base else ConnectOptions()

    if service_name is not None:
        cfg.service_name = service_name
    if ws_url is not None:
        cfg.ws_url = ws_url
    if graphql_url is not None:
        cfg.graphql_url = graphql_url
    if debug is not None:
        cfg.debug = debug
    if max_in_flight is not None:
        cfg.max_in_flight = max_in_flight
    if connect_timeout is not None:
        cfg.connect_timeout = connect_timeout
    if logger is not None:
        cfg.logger = logger

    cfg.with_defaults()
    cfg.validate()
    return cfg


class Threadify:
    """Factory for creating Threadify connections."""

    @staticmethod
    async def connect(
        api_key: str,
        *args: Any,
        service_name: str | None = None,
        ws_url: str | None = None,
        graphql_url: str | None = None,
        debug: bool | None = None,
        max_in_flight: int | None = None,
        connect_timeout: float | None = None,
        logger: logging.Logger | None = None,
        options: ConnectOptions | None = None,
    ) -> Connection:
        require_non_empty("api_key", api_key)

        legacy_service_name: str | None = None
        legacy_config: ConnectOptions | None = None
        for arg in args:
            if isinstance(arg, str) and legacy_service_name is None:
                legacy_service_name = arg
                continue
            if isinstance(arg, ConnectOptions) and legacy_config is None:
                legacy_config = arg
                continue
            raise TypeError(
                "invalid connect argument; expected service_name (str) or ConnectOptions"
            )

        cfg = _build_connect_options(
            base=options or legacy_config,
            service_name=service_name if service_name is not None else legacy_service_name,
            ws_url=ws_url,
            graphql_url=graphql_url,
            debug=debug,
            max_in_flight=max_in_flight,
            connect_timeout=connect_timeout,
            logger=logger,
        )

        ws = await asyncio.wait_for(
            websockets.connect(cfg.ws_url),
            timeout=cfg.connect_timeout,
        )

        connect_msg = {
            FIELD_ACTION: ACTION_CONNECT,
            FIELD_API_KEY: api_key,
            FIELD_SERVICE_NAME: cfg.service_name,
            FIELD_MAX_IN_FLIGHT: cfg.max_in_flight,
        }
        await ws.send(json.dumps(connect_msg))

        raw = await asyncio.wait_for(ws.recv(), timeout=cfg.connect_timeout)
        resp = json.loads(raw)

        if resp.get(FIELD_ACTION) != ACTION_CONNECT or resp.get(FIELD_STATUS) != STATUS_SUCCESS:
            await ws.close()
            msg = resp.get(FIELD_MESSAGE, "connection failed")
            raise ConnectionError(msg)

        conn = Connection(
            ws=ws,
            api_key=api_key,
            service_name=cfg.service_name,
            graphql_url=cfg.graphql_url,
            debug=cfg.debug,
            max_in_flight=cfg.max_in_flight,
            logger=cfg.logger,
        )

        return conn

    @staticmethod
    def create(
        api_key: str,
        *args: Any,
        service_name: str | None = None,
        ws_url: str | None = None,
        graphql_url: str | None = None,
        debug: bool | None = None,
        max_in_flight: int | None = None,
        connect_timeout: float | None = None,
        logger: logging.Logger | None = None,
        options: ConnectOptions | None = None,
    ) -> ThreadifyFactory:
        legacy_service_name: str | None = None
        legacy_config: ConnectOptions | None = None
        for arg in args:
            if isinstance(arg, str) and legacy_service_name is None:
                legacy_service_name = arg
                continue
            if isinstance(arg, ConnectOptions) and legacy_config is None:
                legacy_config = arg
                continue
            raise TypeError(
                "invalid create argument; expected service_name (str) or ConnectOptions"
            )

        cfg = _build_connect_options(
            base=options or legacy_config,
            service_name=service_name if service_name is not None else legacy_service_name,
            ws_url=ws_url,
            graphql_url=graphql_url,
            debug=debug,
            max_in_flight=max_in_flight,
            connect_timeout=connect_timeout,
            logger=logger,
        )
        return ThreadifyFactory(
            api_key=api_key,
            options=cfg,
        )


class ThreadifyFactory:
    def __init__(
        self,
        api_key: str,
        options: ConnectOptions,
    ):
        self._api_key = api_key
        self._options = _copy_connect_options(options)

    async def connect(self) -> Connection:
        return await Threadify.connect(self._api_key, options=self._options)
