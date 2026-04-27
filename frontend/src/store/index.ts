import { configureStore } from "@reduxjs/toolkit";
import { authSlice } from "./authSlice";
import projectsReducer from "@/modules/ProjectManagement/store/projectSlice";
import dataImportReducer from "@/modules/DataImport/store/data-import-slice";
import uiReducer from "./uiSlice";
import dataProfileReducer from "@/modules/DataProfiling/store/data-profile-slice";
import urlParamsReducer from "./urlParamsSlice";

// Profiling SaaS store — slimmed from main-product store during saas-extract.
// Removed slices: license, remoteConnections, dataCleansing, matchConfiguration,
// matchDefinitions, matchResults, mergeSurvivorship, finalExport, scheduler.
export const store = configureStore({
	reducer: {
		auth: authSlice.reducer,
		projects: projectsReducer,
		dataImport: dataImportReducer,
		uiState: uiReducer,
		dataProfile: dataProfileReducer,
		urlParams: urlParamsReducer,
	},
	devTools: { name: "MatchLogic Profiler Store" },
});

export type AppDispatch = typeof store.dispatch;
export type RootState = ReturnType<typeof store.getState>;
