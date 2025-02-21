# Task 1: Basic Visit Counter
 - Use a Python dictionary as an in-memory variable.
 - The key will be `pageid`, and the value will be the `visit-count`.
 - Create a single instance of the `visit_counter_service`.
 - Modify the response-type for Get Api.

### API Endpoints Testing
 1. `POST /api/v1/counter/visit/{page_id}`: Record a visit
![Test 1 Post Call](./images/T1_Post.png)

 2. `GET /api/v1/counter/visits/{page_id}`: Get visit count
 ![Test 2 Get Call](./images/T1_Get.png)

 ## Setup Instructions
 1. Make sure you have Docker and Docker Compose installed
 2. Run the application:
 ```
 docker compose up --build
 ```
 3. The API will be available at `http://localhost:8000`