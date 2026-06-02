"""
Ray monitoring service.

Sử dụng Ray Dashboard State API thay vì ray.init().

Compatible:
- Ray 2.40+
- Dashboard chạy khác Python version với cluster
"""

from __future__ import annotations

import requests
import streamlit as st

from config import RAY_DASHBOARD_URL

TIMEOUT = 5


# =========================================================
# INTERNAL
# =========================================================

def _get(path: str):

    response = requests.get(
        f"{RAY_DASHBOARD_URL}{path}",
        timeout=TIMEOUT
    )

    response.raise_for_status()

    return response.json()


# =========================================================
# VERSION
# =========================================================

@st.cache_data(ttl=10)
def get_version():

    try:

        return _get(
            "/api/version"
        )

    except Exception as e:

        return {
            "error": str(e)
        }


# =========================================================
# NODES
# =========================================================

@st.cache_data(ttl=5)
def get_nodes():

    try:

        payload = _get(
            "/api/v0/nodes"
        )

        data = payload.get(
            "data",
            {}
        )

        if not isinstance(data, dict):
            return []

        result = data.get(
            "result",
            {}
        )

        if not isinstance(result, dict):
            return []

        nodes = result.get(
            "result",
            []
        )

        if not isinstance(nodes, list):
            return []

        return nodes

    except Exception:

        return []


# =========================================================
# JOBS
# =========================================================

@st.cache_data(ttl=5)
def get_jobs():

    try:

        return _get(
            "/api/jobs/"
        )

    except Exception:

        return []


# =========================================================
# ACTORS
# =========================================================

@st.cache_data(ttl=5)
def get_actors():

    try:

        payload = _get(
            "/api/v0/actors"
        )

        return payload.get(
            "data",
            {}
        ).get(
            "result",
            []
        )

    except Exception:

        return []


# =========================================================
# TASKS
# =========================================================

@st.cache_data(ttl=5)
def get_tasks():

    try:

        payload = _get(
            "/api/v0/tasks"
        )

        return payload.get(
            "data",
            {}
        ).get(
            "result",
            []
        )

    except Exception:

        return []


# =========================================================
# RESOURCE SUMMARY
# =========================================================

@st.cache_data(ttl=5)
def cluster_resources():

    nodes = get_nodes()

    cpu = 0
    gpu = 0
    memory = 0
    object_store = 0
    alive = 0

    for node in nodes:

        if node.get("state") == "ALIVE":
            alive += 1

        resources = node.get(
            "resources_total",
            {}
        )

        cpu += resources.get("CPU", 0)
        gpu += resources.get("GPU", 0)

        memory += resources.get(
            "memory",
            0
        )

        object_store += resources.get(
            "object_store_memory",
            0
        )

    return {
        "cpu_total": cpu,
        "gpu_total": gpu,
        "memory_total_gb": memory / (1024**3),
        "object_store_total_gb": object_store / (1024**3),
        "nodes": len(nodes),
        "alive_nodes": alive
    }


# =========================================================
# AVAILABLE RESOURCES
# =========================================================

@st.cache_data(ttl=5)
def available_resources():

    """
    State API không expose available_resources
    như ray.available_resources().

    Tạm thời trả tổng resource.
    """

    return cluster_resources()


# =========================================================
# HEALTH
# =========================================================

@st.cache_data(ttl=5)
def cluster_health():

    try:

        version = get_version()

        if "error" in version:

            return {
                "healthy": False,
                "message": version["error"]
            }

        nodes = get_nodes()

        alive = 0

        for node in nodes:

            if str(
                node.get(
                    "state",
                    ""
                )
            ).upper() == "ALIVE":

                alive += 1

        return {
            "healthy": alive > 0,
            "alive_nodes": alive,
            "total_nodes": len(nodes),
            "ray_version": version.get(
                "ray_version",
                "unknown"
            )
        }

    except Exception as e:

        return {
            "healthy": False,
            "message": str(e)
        }