from typing import Annotated, Any

from fastapi import APIRouter, Depends, File, Form, Query, UploadFile
from sqlalchemy.ext.asyncio import AsyncSession

from api.simulation.schemas import (
    SimulateRequest,
    SimulateResponse,
    Simulation,
    SimulationDetail,
    SimulationTimeseries,
)
from api.simulation.service import SimulationService
from core.api_response import ApiResponse, ApiResponsePaginated
from core.context_vars import current_internal_community_id
from core.database.database import get_crm_session, get_local_session
from core.errors.errors import ErrorException
from core.errors.with_default_error import with_default_error
from core.security.community_scope import resolve_internal_community
from core.security.dependencies import require_feature
from shared.const import FeatureName
from shared.custom_errors import errors
from shared.helpers.parse_query_params import parse_query_params

simulation_routes = APIRouter(
    dependencies=[
        Depends(resolve_internal_community),
        Depends(require_feature(FeatureName.SIMULATION)),
    ]
)


# GET (/) : List simulations and their status (pending, success, failed).
@simulation_routes.get("/", response_model=ApiResponsePaginated[list[Simulation]])
@with_default_error(default_error=errors.simulation.GET_SIMULATIONS)
async def get_simulations(
    query_param: Annotated[dict[str, Any], Depends(parse_query_params)],
    local_session: Annotated[AsyncSession, Depends(get_local_session)],
    crm_session: Annotated[AsyncSession, Depends(get_crm_session)],
    page: Annotated[int, Query(description="Page number.")] = 1,
    page_size: Annotated[int, Query(description="Page size.")] = 20,
):
    service = SimulationService(local_session, crm_session)
    data, pagination = await service.get_simulations(page, page_size, query_param)
    return ApiResponsePaginated[list[Simulation]](data=data, pagination=pagination)


# GET (/{id}) : One simulation with its scalar result tree.
@simulation_routes.get("/{id}", response_model=ApiResponse[SimulationDetail])
@with_default_error(default_error=errors.simulation.GET_SIMULATION)
async def get_simulation(
    id: int,
    local_session: Annotated[AsyncSession, Depends(get_local_session)],
    crm_session: Annotated[AsyncSession, Depends(get_crm_session)],
):
    service = SimulationService(local_session, crm_session)
    data = await service.get_simulation(id)
    return ApiResponse[SimulationDetail](data=data)


# GET (/{id}/timeseries) : Per-timestep series for charting.
@simulation_routes.get("/{id}/timeseries", response_model=ApiResponse[SimulationTimeseries])
@with_default_error(default_error=errors.simulation.GET_TIMESERIES)
async def get_timeseries(
    id: int,
    local_session: Annotated[AsyncSession, Depends(get_local_session)],
    crm_session: Annotated[AsyncSession, Depends(get_crm_session)],
):
    service = SimulationService(local_session, crm_session)
    data = await service.get_timeseries(id)
    return ApiResponse[SimulationTimeseries](data=data)


# POST (/) : Launch a simulation.
# Multipart/form-data: the file is uploaded alongside the metadata.
@simulation_routes.post("/", response_model=ApiResponse[SimulateResponse])
@with_default_error(default_error=errors.simulation.START_SIMULATION)
async def start_simulation(
    local_session: Annotated[AsyncSession, Depends(get_local_session)],
    crm_session: Annotated[AsyncSession, Depends(get_crm_session)],
    file: Annotated[UploadFile, File(description="Consumption/production CSV/XLSX file.")],
    name: Annotated[str, Form(description="User-facing label for the simulation.")],
    id_key: Annotated[int, Form(description="CRM allocation key id to stress-test.")],
    injection_name: Annotated[
        str, Form(description="Injection (production) column name in the file.")
    ],
):
    req = SimulateRequest(name=name, id_key=id_key, injection_name=injection_name)
    # resolve_internal_community (router dep) has cached the internal CRM id.
    internal_community_id = current_internal_community_id.get()
    if internal_community_id is None:
        raise ErrorException(error=errors.auth.UNAUTHORIZED, status_code=401)
    service = SimulationService(local_session, crm_session)
    data = await service.start_simulation(req, file, internal_community_id)
    return ApiResponse[SimulateResponse](data=data)


# DELETE (/{id}) : Delete a simulation and its results.
@simulation_routes.delete("/{id}", response_model=ApiResponse[str])
@with_default_error(default_error=errors.simulation.DELETE_SIMULATION)
async def delete_simulation(
    id: int,
    local_session: Annotated[AsyncSession, Depends(get_local_session)],
    crm_session: Annotated[AsyncSession, Depends(get_crm_session)],
):
    service = SimulationService(local_session, crm_session)
    await service.delete_simulation(id)
    return ApiResponse[str](data="success")
