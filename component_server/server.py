#!python3

from copy import copy
from typing import Optional, Type

import sqlalchemy.inspection
from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel, create_model
from sqlalchemy.orm import DeclarativeBase, Query, Session

from component_server import models
from component_server.session import SessionLocal

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
        columns[column.name] = (Optional[column.type.python_type], None)  # No default

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

CapacitorRequest = _create_pydantic_request_model(models.Capacitor)
CapacitorResponse = _create_pydantic_response_model(models.Capacitor)


@app.post("/v2/find/capacitor", response_model=CapacitorResponse)
async def get_capacitor(request: CapacitorRequest, db: Session = Depends(get_db)):
    """
    Get a capacitor based on the capacitor specs.
    If is a spec is omitted, it will allow any value for that spec in the search.
    """
    return _find_component(models.Capacitor, request, db)


@app.post("/v2/find/diagnose/capacitor", response_model=DiagnosticReport)
async def get_capacitor_diagnostic(
    request: CapacitorRequest, db: Session = Depends(get_db)
):
    """
    Get a diagnostics report on a capacitor search.
    """
    return _do_diag(models.Capacitor, request, db)


# #####################
# # Inductor endpoint
# #####################

InductorRequest = _create_pydantic_request_model(models.Inductor)
InductorResponse = _create_pydantic_response_model(models.Inductor)


@app.post("/v2/find/inductor", response_model=InductorResponse)
async def get_inductors(request: InductorRequest, db: Session = Depends(get_db)):
    """
    Get a inductor based on the inductor specs.
    If is a spec is omitted, it will allow any value for that spec in the search.
    """
    return _find_component(models.Inductor, request, db)


@app.post("/v2/find/diagnose/inductor", response_model=DiagnosticReport)
async def get_inductors_diagnostic(
    request: InductorRequest, db: Session = Depends(get_db)
):
    """
    Get a diagnostics report on a inductor search.
    """
    return _do_diag(models.Inductor, request, db)


# #####################
# # Inductor endpoint
# #####################

MosfetRequest = _create_pydantic_request_model(models.Mosfet)
MosfetResponse = _create_pydantic_response_model(models.Mosfet)


@app.post("/v2/find/mosfet", response_model=MosfetResponse)
async def get_mosfet(request: MosfetRequest, db: Session = Depends(get_db)):
    """
    Get a mosfet based on the mosfet specs.
    If is a spec is omitted, it will allow any value for that spec in the search.
    """
    return _find_component(models.Mosfet, request, db)


@app.post("/v2/find/diagnose/mosfet", response_model=DiagnosticReport)
async def get_mosfet_diagnostic(
    request: MosfetRequest, db: Session = Depends(get_db)
):
    """
    Get a diagnostics report on a mosfet search.
    """
    return _do_diag(models.Mosfet, request, db)


# #####################
# # Main
# #####################

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
