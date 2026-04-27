import { createSlice, PayloadAction } from "@reduxjs/toolkit";

interface UIState {
  loadingCount: number;
  showLoader: boolean;
  error?: string;
  sidebarExpanded: boolean;
}

const initialState: UIState = {
  loadingCount: 0,
  showLoader: false,
  error: undefined,
  sidebarExpanded: false,
};

const uiSlice = createSlice({
  name: "ui",
  initialState,
  reducers: {
    // Increments the raw loading count
    startLoading(state) {
      state.loadingCount += 1;
    },
    // Decrements the raw loading count
    finishLoading(state) {
      if (state.loadingCount > 0) {
        state.loadingCount -= 1;
      }
      // If no more in-flight requests, hide the spinner
      if (state.loadingCount === 0) {
        state.showLoader = false;
      }
    },
    // Controls whether the spinner should actually be shown
    setShowLoader(state, action: PayloadAction<boolean>) {
      state.showLoader = action.payload;
    },
    setError(state, action: PayloadAction<string>) {
      state.error = action.payload;
    },
    clearError(state) {
      state.error = undefined;
    },
    toggleSidebar(state) {
      state.sidebarExpanded = !state.sidebarExpanded;
    },
    setSidebarExpanded(state, action: PayloadAction<boolean>) {
      state.sidebarExpanded = action.payload;
    },
  },
});

export const {
  startLoading,
  finishLoading,
  setShowLoader,
  setError,
  clearError,
  toggleSidebar,
  setSidebarExpanded,
} = uiSlice.actions;
export default uiSlice.reducer;
