#!python3

from copy import copy
from typing import Optional, Type

import sqlalchemy.inspection
from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel, create_model
from sqlalchemy.orm import DeclarativeBase, Query, Session

from src import models
from src.session import SessionLocal

app = FastAPI()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.get("/")
async def root():
    return (
        "This is the atopile component server! It's meant to be queried to the ato CLI"
    )


# Dynamically generate the pydantic models from the SQLAlchemy models
def _create_pydantic_request_model(model: Type[DeclarativeBase]) -> BaseModel:
    columns = {}
    for column in sqlalchemy.inspection.inspect(model).columns:
        query_operator = column.info.get("query_operator")
        if query_operator is None:
            continue

        # For things that are strings (practically an enum), we should be able to query from a list
        if query_operator is models.in_:
            python_type = list[column.type.python_type]
        else:
            python_type = column.type.python_type

        columns[column.name] = (Optional[python_type], None)  # No default
    return create_model(model.__name__, **columns, __config__={"from_attributes": True})


def _create_pydantic_response_model(model: Type[DeclarativeBase]) -> BaseModel:
    columns = {}
    for column in sqlalchemy.inspection.inspect(model).columns:
        if not column.info.get("return", False):
            continue
        columns[column.name] = (column.type.python_type, None)  # No default

    return create_model(model.__name__, **columns, __config__={"from_attributes": True})


def _filter_components(
    model: Type[DeclarativeBase], request: BaseModel, query: Query
) -> Query:
    """Apply filtering based on the database scheme and query data."""
    # Apply filtering based on the query
    for column in sqlalchemy.inspection.inspect(model).columns:
        query_operator = column.info.get("query_operator")
        if query_operator is None:
            continue

        query_value = getattr(request, column.name)
        if query_value is None:
            continue

        query = query.where(query_operator(getattr(model, column.name), query_value))
    return query


def _find_component(model: Type[DeclarativeBase], request: BaseModel, db: Session):
    assert hasattr(model, "rating"), f"Component {model} is missing a rating column"

    with db as session:
        db_query = session.query(model)
        db_query = _filter_components(model, request, db_query)

        # Execute the query
        candidate = db_query.first()
        if candidate is None:
            raise HTTPException(status_code=500, detail="No component found")

        return candidate


# Diagnosis tools to figure out what's over-constraining params
class DiagnosticReport(BaseModel):
    """A report of the number of results, if we removed each of the filters, one at a time."""

    results_without_filter: dict[str, int]


def _do_diag(
    model: Type[DeclarativeBase], request: BaseModel, db: Session
) -> DiagnosticReport:
    report = {}
    for name in request.model_fields:
        # Skip fields for which the request is already None
        # We can't de-restrict these further
        if getattr(request, name) is None:
            continue

        # Create a copy of the request, but with the field set to None
        new_request = copy(request)
        setattr(new_request, name, None)

        # Filter for components without this constraint
        with db as session:
            db_query = session.query(model)
            db_query = _filter_components(model, new_request, db_query)
            count = db_query.count()
            report[name] = count

    return DiagnosticReport(results_without_filter=report)


#####################
# Resistor end point
#####################

ResistorRequest = _create_pydantic_request_model(models.Resistor)
ResistorResponse = _create_pydantic_response_model(models.Resistor)


@app.post("/v2/find/resistor", response_model=ResistorResponse)
async def get_resistor(request: ResistorRequest, db: Session = Depends(get_db)):
    """
    Get a resistor based on the resistor specs.
    If is a spec is omitted, it will allow any value for that spec in the search.
    """
    return _find_component(models.Resistor, request, db)


@app.post("/v2/find/diagnose/resistor", response_model=DiagnosticReport)
async def get_resistor_diagnostic(
    request: ResistorRequest, db: Session = Depends(get_db)
):
    """
    Get a diagnostics report on a resistor search.
    """
    return _do_diag(models.Resistor, request, db)


# #####################
# # Capacitor endpoint
# #####################


# def capacitor_capacitance_filter(query: Query, api_data: api_schema.Capacitor):
#     return query.filter(
#         models.Capacitor.capacitance_min_farads >= api_data.capacitance_farads.min,
#         models.Capacitor.capacitance_max_farads <= api_data.capacitance_farads.max,
#     )


# def capacitor_voltage_rating_filter(query: Query, api_data: api_schema.Capacitor):
#     return query.filter(
#         models.Capacitor.voltage_rating_min_volts
#         <= api_data.voltage_rating_volts.min,
#         models.Capacitor.voltage_rating_max_volts
#         >= api_data.voltage_rating_volts.max,
#     )


# def capacitor_equivalent_series_resistance_filter(
#     query: Query, api_data: api_schema.Capacitor
# ):
#     if api_data.equivalent_series_resistance_ohms is None:
#         return query
#     return query.filter(
#         models.Capacitor.equivalent_series_resistance_min_ohms
#         >= api_data.equivalent_series_resistance_ohms.min,
#         models.Capacitor.equivalent_series_resistance_max_ohms
#         <= api_data.equivalent_series_resistance_ohms.max,
#     )


# def capacitor_temperature_coefficient_filter(
#     query: Query, api_data: api_schema.Capacitor
# ):
#     if api_data.temperature_coefficient_farads_per_celsius is None:
#         return query
#     return query.filter(
#         models.Capacitor.temperature_coefficient_min_farads_per_celsius
#         <= api_data.temperature_coefficient_farads_per_celsius.min,
#         models.Capacitor.temperature_coefficient_max_farads_per_celsius
#         >= api_data.temperature_coefficient_farads_per_celsius.max,
#     )


# capacitor_filters = [
#     capacitor_capacitance_filter,
#     capacitor_voltage_rating_filter,
#     capacitor_equivalent_series_resistance_filter,
#     capacitor_temperature_coefficient_filter,
# ]


# @app.post("/capacitor", response_model=api_schema.CapacitorOutbound)
# async def get_capacitor(
#     inbound_data: api_schema.CapacitorInbound, db: Session = Depends(get_db)
# ):
#     """
#     Get a capacitor based on requirements.
#     """
#     try:
#         with db as session:
#             query = session.query(models.Capacitor)

#             for filter in common_filters:
#                 query = filter(query, models.Capacitor, inbound_data)

#             for filter in capacitor_filters:
#                 query = filter(query, inbound_data)

#             # Sort the candidates by rating
#             query = query.order_by(models.Capacitor.rating.desc())

#             # Execute the query
#             candidate = query.first()
#             if candidate is None:
#                 raise HTTPException(status_code=404, detail="No capacitor found")
#             return candidate
#     except HTTPException as e:
#         raise HTTPException(status_code=e.status_code, detail=e.detail)


# @app.post("/capacitor/diagnostic", response_model=api_schema.DiagnosticReport)
# async def get_capacitor_diagnostic(
#     inbound_data: api_schema.CapacitorQuery, db: Session = Depends(get_db)
# ):
#     """
#     Get a diagnostics report on a capacitor search.
#     """
#     try:
#         # TODO: ---
#         with db as session:
#             filter_count_results = []
#             for filter_func in common_filters:
#                 query = session.query(models.Capacitor)
#                 query = filter_func(query, models.Capacitor, inbound_data)
#                 count = query.count()
#                 filter_count_results.append(
#                     api_schema.FilterResults(
#                         filter_name=filter_func.__name__, count=count
#                     )
#                 )
#             for filter_func in capacitor_filters:
#                 query = session.query(models.Capacitor)
#                 query = filter_func(query, inbound_data)
#                 count = query.count()
#                 filter_count_results.append(
#                     api_schema.FilterResults(
#                         filter_name=filter_func.__name__, count=count
#                     )
#                 )

#             # Check if results are available with other packages
#             query = session.query(models.Capacitor)
#             query = temperature_rating_filter(query, models.Capacitor, inbound_data)
#             for filter in capacitor_filters:
#                 query = filter(query, inbound_data)
#             count = query.count()
#             can_find_results_with_other_packages = False
#             if count > 0:
#                 can_find_results_with_other_packages = True

#             report = api_schema.DiagnosticReport(
#                 component_type="capacitor",
#                 search_parameters=inbound_data,
#                 individual_filter_results=filter_count_results,
#                 can_find_results_with_other_packages=can_find_results_with_other_packages,
#             )

#             return report
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))


# #####################
# # Inductor endpoint
# #####################


# def inductor_inductance_filter(query: Query, api_data: api_schema.Inductor):
#     return query.filter(
#         models.Inductor.inductance_min_henry >= api_data.inductance_henry.min,
#         models.Inductor.inductance_max_henry <= api_data.inductance_henry.max,
#     )


# def inductor_current_rating_filter(query: Query, api_data: api_schema.Inductor):
#     return query.filter(
#         models.Inductor.current_rating_min_amperes
#         >= api_data.current_rating_amperes.min,
#         models.Inductor.current_rating_max_amperes
#         <= api_data.current_rating_amperes.max,
#     )


# def inductor_saturation_current_filter(query: Query, api_data: api_schema.Inductor):
#     if api_data.saturation_current_amperes is None:
#         return query
#     return query.filter(
#         models.Inductor.saturation_current_min_amperes
#         >= api_data.saturation_current_amperes.min,
#         models.Inductor.saturation_current_max_amperes
#         <= api_data.saturation_current_amperes.max,
#     )


# def inductor_rms_current_filter(query: Query, api_data: api_schema.Inductor):
#     if api_data.rms_current_amperes is None:
#         return query
#     return query.filter(
#         models.Inductor.rms_current_min_amperes >= api_data.rms_current_amperes.min,
#         models.Inductor.rms_current_max_amperes <= api_data.rms_current_amperes.max,
#     )


# def inductor_resonant_frequency_filter(query: Query, api_data: api_schema.Inductor):
#     if api_data.resonant_frequency_hertz is None:
#         return query
#     return query.filter(
#         models.Inductor.resonant_frequency_min_hertz
#         >= api_data.resonant_frequency_hertz.min,
#         models.Inductor.resonant_frequency_max_hertz
#         <= api_data.resonant_frequency_hertz.max,
#     )


# def inductor_resistance_filter(query: Query, api_data: api_schema.Inductor):
#     if api_data.resistance_ohms is None:
#         return query
#     return query.filter(
#         models.Inductor.resistance_min_ohms >= api_data.resistance_ohms.min,
#         models.Inductor.resistance_max_ohms <= api_data.resistance_ohms.max,
#     )


# def inductor_temperature_coefficient_filter(
#     query: Query, api_data: api_schema.Inductor
# ):
#     if api_data.temperature_coefficient_henry_per_celsius is None:
#         return query
#     return query.filter(
#         models.Inductor.temperature_coefficient_min_henry_per_celsius
#         >= api_data.temperature_coefficient_henry_per_celsius.min,
#         models.Inductor.temperature_coefficient_max_henry_per_celsius
#         <= api_data.temperature_coefficient_henry_per_celsius.max,
#     )


# inductor_filters = [
#     inductor_inductance_filter,
#     inductor_current_rating_filter,
#     inductor_saturation_current_filter,
#     inductor_rms_current_filter,
#     inductor_resonant_frequency_filter,
#     inductor_resistance_filter,
#     inductor_temperature_coefficient_filter,
# ]


# @app.post("/inductor", response_model=api_schema.InductorResponse)
# async def get_inductor(
#     inbound_data: api_schema.InductorQuery, db: Session = Depends(get_db)
# ):
#     """
#     Get an inductor based on requirements.
#     """
#     try:
#         with db as session:
#             query = session.query(models.Inductor)

#             for filter in common_filters:
#                 query = filter(query, models.Inductor, inbound_data)

#             for filter in inductor_filters:
#                 query = filter(query, inbound_data)

#             # Sort the candidates by rating
#             query = query.order_by(models.Inductor.rating.desc())

#             # Execute the query
#             candidate = query.first()
#             if candidate is None:
#                 raise HTTPException(status_code=404, detail="No capacitor found")
#             return candidate
#     except HTTPException as e:
#         raise HTTPException(status_code=e.status_code, detail=e.detail)


# @app.post("/inductor/diagnostic", response_model=api_schema.DiagnosticReport)
# async def get_inductor_diagnostic(
#     inbound_data: api_schema.InductorQuery, db: Session = Depends(get_db)
# ):
#     """
#     Get a diagnostics report on an inductor search.
#     """
#     try:
#         with db as session:
#             filter_count_results = []
#             for filter_func in common_filters:
#                 query = session.query(models.Inductor)
#                 query = filter_func(query, models.Inductor, inbound_data)
#                 count = query.count()
#                 filter_count_results.append(
#                     api_schema.FilterResults(
#                         filter_name=filter_func.__name__, count=count
#                     )
#                 )
#             for filter_func in inductor_filters:
#                 query = session.query(models.Inductor)
#                 query = filter_func(query, inbound_data)
#                 count = query.count()
#                 filter_count_results.append(
#                     api_schema.FilterResults(
#                         filter_name=filter_func.__name__, count=count
#                     )
#                 )

#             # Check if results are available with other packages
#             query = session.query(models.Inductor)
#             query = temperature_rating_filter(query, models.Inductor, inbound_data)
#             for filter in inductor_filters:
#                 query = filter(query, inbound_data)
#             count = query.count()
#             can_find_results_with_other_packages = False
#             if count > 0:
#                 can_find_results_with_other_packages = True

#             report = api_schema.DiagnosticReport(
#                 component_type="inductor",
#                 search_parameters=inbound_data,
#                 individual_filter_results=filter_count_results,
#                 can_find_results_with_other_packages=can_find_results_with_other_packages,
#             )

#             return report
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
