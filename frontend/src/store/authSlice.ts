import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import type { RootState } from "@/store";

export interface AuthState {
  isAuthenticated: boolean;
  isInitialized: boolean;
  userId: string | null;
  username: string | null;
  email: string | null;
  roles: string[];
  permissions: string[];
  permissionsLoaded: boolean;
}

const initialState: AuthState = {
  isAuthenticated: false,
  isInitialized: false,
  userId: null,
  username: null,
  email: null,
  roles: [],
  permissions: [],
  permissionsLoaded: false,
};

export const authSlice = createSlice({
  name: "auth",
  initialState,
  reducers: {
    setAuthState: (state, action: PayloadAction<Partial<Omit<AuthState, "permissions" | "permissionsLoaded">>>) => {
      return { ...state, ...action.payload };
    },
    setPermissions: (state, action: PayloadAction<string[]>) => {
      state.permissions = action.payload;
      state.permissionsLoaded = true;
    },
    clearAuth: () => initialState,
  },
});

export const { setAuthState, setPermissions, clearAuth } = authSlice.actions;

export const selectIsAuthenticated = (state: RootState) =>
  state.auth.isAuthenticated;
export const selectIsInitialized = (state: RootState) =>
  state.auth.isInitialized;
export const selectCurrentUser = (state: RootState) => ({
  userId: state.auth.userId,
  username: state.auth.username,
  email: state.auth.email,
  roles: state.auth.roles,
});
export const selectPermissions = (state: RootState) => state.auth.permissions;
export const selectPermissionsLoaded = (state: RootState) =>
  state.auth.permissionsLoaded;
