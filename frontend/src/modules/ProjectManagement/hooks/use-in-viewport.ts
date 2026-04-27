import { useEffect, useRef, useState } from "react";

/**
 * Returns a ref + boolean indicating whether the element has entered the viewport.
 * Once `hasBeenVisible` becomes true, it stays true (one-shot trigger).
 * This avoids refetching when user scrolls past and back.
 */
export function useInViewport<T extends HTMLElement = HTMLDivElement>() {
	const ref = useRef<T>(null);
	const [hasBeenVisible, setHasBeenVisible] = useState(false);

	useEffect(() => {
		const el = ref.current;
		if (!el || hasBeenVisible) return;

		const observer = new IntersectionObserver(
			([entry]) => {
				if (entry.isIntersecting) {
					setHasBeenVisible(true);
					observer.disconnect();
				}
			},
			{ rootMargin: "100px" } // trigger slightly before card is visible
		);

		observer.observe(el);
		return () => observer.disconnect();
	}, [hasBeenVisible]);

	return { ref, hasBeenVisible };
}
