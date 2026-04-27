import { createSlice, PayloadAction } from "@reduxjs/toolkit"

const initialState = {
    dataSourceId: "",
    selectedTab: "overview",
    viewMode: "standard" as "standard" | "numeric"
}

const dataProfileSlice = createSlice({
    name: "data-profile",
    initialState,
    reducers: {
        setDataSourceId: (state, action: PayloadAction<string>) => {
            state.dataSourceId = action.payload
        },
        setSelectedTab: (state, action: PayloadAction<string>) => {
            state.selectedTab = action.payload
        },
        setViewMode: (state, action: PayloadAction<"standard" | "numeric">) => {
            state.viewMode = action.payload
        }
    }
})

export const dataProfileActions = dataProfileSlice.actions
export default dataProfileSlice.reducer