"use client"

import {
	createSlice,
	createAsyncThunk,
	type PayloadAction,
} from "@reduxjs/toolkit";
import { queryClient } from "@/store/query-client";
import { Project } from "@/models/api-responses";

export type ViewMode = "cards" | "list";

interface ProjectState {
	projects: Project[];
	status: "idle" | "loading" | "succeeded" | "failed";
	error: string | null;
	selectedProject: Project | null;
	apiConnected: boolean;
	viewMode: ViewMode;
	searchTerm: string;
	createModalOpen: boolean;
	sortConfig: {
		key: "modifiedAt" | "createdAt";
		direction: "asc" | "desc";
	}
}

const initialState: ProjectState = {
	projects: [],
	status: "idle",
	error: null,
	selectedProject: null,
	apiConnected: true,
	viewMode: "list",
	searchTerm: "",
	createModalOpen: false,
	sortConfig: {
		key: "createdAt",
		direction: "desc"
	}
};


const projectSlice = createSlice({
	name: "projects",
	initialState,
	reducers: {
		setStatus: (state, action: PayloadAction<"loading" | "idle" | "succeeded" | "failed">) => {
			state.status = action.payload;
			if (action.payload == "failed") {
				state.apiConnected = false;
			}
		},
		setSelectedProject: (state, action: PayloadAction<Project | null>) => {
			if (state.selectedProject?.id !== action.payload?.id) {
				queryClient.clear()
			}
			state.selectedProject = action.payload;
		},
		setViewMode: (state, action: PayloadAction<ViewMode>) => {
			state.viewMode = action.payload;
		},
		setSearchTerm: (state, action: PayloadAction<string>) => {
			state.searchTerm = action.payload;
		},
		setCreateModalOpen: (state, action: PayloadAction<boolean>) => {
			state.createModalOpen = action.payload;
		},
		setSortConfig: (state, action: PayloadAction<{ key: "modifiedAt" | "createdAt" }>) => {
			if (state.sortConfig.key === action.payload.key) {
				state.sortConfig = { key: action.payload.key, direction: state.sortConfig.direction === "asc" ? "desc" : "asc" };
			} else {
				state.sortConfig = { key: action.payload.key, direction: "desc" };
			}
		},
		clearError: (state) => {
			state.error = null;
		},
	}
});

export const projectActions = projectSlice.actions;
export default projectSlice.reducer;
