import { Router } from "express";
import { checkifBusinessIsBanned } from "../controllers/bannerController.js";
const router = Router();

router.post("/business", checkifBusinessIsBanned);

export default router;