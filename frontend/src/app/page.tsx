import React from "react";
import ProjectManagementPage from "./project-management/page";
import { redirect } from "next/navigation";

export default function App() {
  return redirect("/project-management")
}