import { SVGProps } from "react";

export function GeneralArrowRightHoverIcon(props: SVGProps<SVGSVGElement>) {
  return (
    <svg
      width="16"
      height="16"
      viewBox="0 0 16 16"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      {...props}
    >
      <rect width="16" height="16" fill="white" fillOpacity="0.01" />
      <path
        d="M5 3V4H11.295L3 12.295L3.705 13L12 4.705V11H13V3H5Z"
        fill="currentColor"
      />
    </svg>
  );
}
