import { ProfileResult } from "./profile-result"
import { BasicResponse } from "@/models/basic-response"

export interface AdvanceAnalytics extends BasicResponse {
  value: {
    profileResult: ProfileResult
  } | null
}

