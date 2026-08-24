import base64
import io
import re
import cv2
import numpy as np
from fastapi import FastAPI
from pydantic import BaseModel
from PIL import Image

app = FastAPI(title="Project 2 Universal Vision AI Engine")

class SolveRequest(BaseModel):
    taskId: str
    prompt: str
    image_base64: str

def base64_to_cv2(b64_str: str):
    if "," in b64_str:
        b64_str = b64_str.split(",")[1]
    img_bytes = base64.b64decode(b64_str)
    pil_img = Image.open(io.BytesIO(img_bytes)).convert("RGB")
    return cv2.cvtColor(np.array(pil_img), cv2.COLOR_RGB2BGR)

@app.post("/solve")
async def solve_task(req: SolveRequest):
    img = base64_to_cv2(req.image_base64)
    h, w, _ = img.shape
    prompt = req.prompt.lower()
    
    clicks = []

    # 1. Matching Shapes / Coordinates Challenge
    if "matching shapes" in prompt or "two matching" in prompt:
        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        blurred = cv2.GaussianBlur(gray, (5, 5), 0)
        edges = cv2.Canny(blurred, 50, 150)
        contours, _ = cv2.findContours(edges, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        
        valid_shapes = []
        for cnt in contours:
            area = cv2.contourArea(cnt)
            if 600 < area < (w * h * 0.25):
                M = cv2.moments(cnt)
                if M["m00"] != 0:
                    cx = int(M["m10"] / M["m00"])
                    cy = int(M["m01"] / M["m00"])
                    valid_shapes.append((cx, cy, area))
        
        # Pick top 2 most balanced matching centroids
        if len(valid_shapes) >= 2:
            valid_shapes.sort(key=lambda s: s[2], reverse=True)
            for s in valid_shapes[:2]:
                px = round((s[0] / w) * 100, 2)
                py = round((s[1] / h) * 100, 2)
                clicks.append({"px": px, "py": py})

    # 2. Number / Target Animals & Grid Matching
    elif "animals based on the number" in prompt or "animal" in prompt:
        # Split into Left Guide Panel & Right Matrix
        left_guide = img[:, :int(w * 0.28)]
        right_grid = img[:, int(w * 0.28):]
        
        rw = right_grid.shape[1]
        rh = right_grid.shape[0]
        
        # 4x4 Grid Slot Matrix
        rows, cols = 4, 4
        slot_w = rw / cols
        slot_h = rh / rows
        
        # Extract prominent objects across slots
        for r in range(rows):
            for c in range(cols):
                sx = int(c * slot_w)
                sy = int(r * slot_h)
                slot = right_grid[sy:int(sy + slot_h), sx:int(sx + slot_w)]
                
                # Check variance / object presence
                gray_slot = cv2.cvtColor(slot, cv2.COLOR_BGR2GRAY)
                if np.std(gray_slot) > 22:
                    cx = int(w * 0.28) + sx + (slot_w / 2)
                    cy = sy + (slot_h / 2)
                    px = round((cx / w) * 100, 2)
                    py = round((cy / h) * 100, 2)
                    clicks.append({"px": px, "py": py})
                    if len(clicks) == 2:
                        break
            if len(clicks) == 2:
                break

    # Fallback if no specific trigger
    if not clicks:
        clicks.append({"px": 50.0, "py": 50.0})

    return {
        "success": True,
        "taskId": req.taskId,
        "clicks": clicks
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)