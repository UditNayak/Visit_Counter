from fastapi import APIRouter, HTTPException, Depends
from ....services.visit_counter import VisitCounterService

router = APIRouter()

# Create a single instance that persists across requests
visit_counter_service = VisitCounterService()

# Dependency to get VisitCounterService instance
def get_visit_counter_service():
    return visit_counter_service

@router.post("/visit/{page_id}")
async def record_visit(
    page_id: str,
    counter_service: VisitCounterService = Depends(get_visit_counter_service)
):
    """Record a visit for a website"""
    try:
        await counter_service.increment_visit(page_id)
        return {"status": "success", "message": f"Visit recorded for page {page_id}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/visits/{page_id}")
async def get_visits(
    page_id: str,
    counter_service: VisitCounterService = Depends(get_visit_counter_service)
):
    """Get visit count and source for a website"""
    try:
        visit_data = await counter_service.get_visit_count(page_id)
        return {
            "visits": visit_data["visits"],
            "served_via": visit_data["served_via"]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
