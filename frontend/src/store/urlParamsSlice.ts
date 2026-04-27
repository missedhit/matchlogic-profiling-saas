import { createSlice, createAsyncThunk, type PayloadAction } from "@reduxjs/toolkit";
import { RootState, AppDispatch } from "./index";

// URL parameter types
export interface URLParams {
  projectId?: string;
  workflowId?: string;
  runId?: string;
  page?: string;
  tab?: string;
  [key: string]: string | undefined;
}

// Route configuration - maps URL params to store actions
export interface RouteConfig {
  projectId?: {
    action: (dispatch: AppDispatch, projectId: string) => Promise<void>;
    selector: (state: RootState) => string | undefined;
  };
  workflowId?: {
    action: (dispatch: AppDispatch, workflowId: string) => Promise<void>;
    selector: (state: RootState) => string | undefined;
  };
  [key: string]: {
    action: (dispatch: AppDispatch, value: string) => Promise<void>;
    selector: (state: RootState) => string | undefined;
  } | undefined;
}

interface URLParamsState {
  params: URLParams;
  isInitialized: boolean;
  isSyncing: boolean;
}

const initialState: URLParamsState = {
  params: {},
  isInitialized: false,
  isSyncing: false,
};

// Async thunk to handle URL parameter changes and trigger corresponding actions
export const syncURLParams = createAsyncThunk<
  void,
  { params: URLParams; routeConfig?: RouteConfig },
  { rejectValue: string; dispatch: AppDispatch; state: RootState }
>(
  "urlParams/sync",
  async ({ params, routeConfig }, { dispatch, getState, rejectWithValue }) => {
    try {
      const currentState = getState().urlParams.params;
      
      // Check if projectId changed and we have a route config for it
      // OR if this is the first time we're seeing this projectId (store was empty)
      if (
        params.projectId &&
        routeConfig?.projectId &&
        (params.projectId !== currentState.projectId || Object.keys(currentState).length === 1)
      ) {
        await routeConfig.projectId.action(dispatch, params.projectId);
      }

      // Handle other parameters dynamically
      for (const key of Object.keys(params)) {
        const value = params[key];
        const currentValue = currentState[key];
        const config = routeConfig?.[key];
        
        if (value && value !== currentValue && config && key !== 'projectId') {
          await config.action(dispatch, value);
        }
      }
    } catch (error) {
      return rejectWithValue("Failed to sync URL parameters");
    }
  }
);

const urlParamsSlice = createSlice({
  name: "urlParams",
  initialState,
  reducers: {
    setURLParams: (state, action: PayloadAction<URLParams>) => {
      state.params = { ...state.params, ...action.payload };
    },
    clearURLParams: (state) => {
      state.params = {};
    },
    setInitialized: (state, action: PayloadAction<boolean>) => {
      state.isInitialized = action.payload;
    },
    updateSingleParam: (
      state,
      action: PayloadAction<{ key: keyof URLParams; value: string | undefined }>
    ) => {
      if (action.payload.value === undefined) {
        delete state.params[action.payload.key];
      } else {
        state.params[action.payload.key] = action.payload.value;
      }
    },
  },
  extraReducers: (builder) => {
    builder
      .addCase(syncURLParams.pending, (state) => {
        state.isSyncing = true;
      })
      .addCase(syncURLParams.fulfilled, (state) => {
        state.isSyncing = false;
      })
      .addCase(syncURLParams.rejected, (state) => {
        state.isSyncing = false;
      });
  },
});

export const { setURLParams, clearURLParams, setInitialized, updateSingleParam } =
  urlParamsSlice.actions;
export default urlParamsSlice.reducer;

// Selectors
export const selectURLParams = (state: RootState) => state.urlParams.params;
export const selectIsURLParamsInitialized = (state: RootState) => 
  state.urlParams.isInitialized;
export const selectIsURLParamsSyncing = (state: RootState) => 
  state.urlParams.isSyncing;