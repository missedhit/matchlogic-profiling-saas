import { ValidationError } from "./validation-error"

export interface BasicResponse {
    status: number
    isSuccess: boolean
    successMessage: string
    correlationId: string
    location: string
    errors: string[]
    validationErrors: ValidationError[]
}