# Illustration guide

[Open the offline gallery](gallery.html). SVGs are editable sources; PNGs are exported at 2400 pixels wide. The gallery uses the same artwork without external services or fonts.

## AIS reception network

[How AIS works](ais/01-how-ais-works.svg): Class A and Class B vessel transponders, ship-to-ship and shore reception, and low Earth orbit satellite collection with a ground downlink. [Standalone preview](ais/01-how-ais-works.html).

## Identity and tracks

1. [Shared MMSI](identity/01-shared-mmsi.svg): preserve independent movement sequences for vessels reporting the same identifier.
2. [Late bridge](identity/02-late-bridge.svg): late A10 changes earlier track assignments.
3. [Two time boundaries](identity/03-two-time-boundaries.svg): the correction window and maximum link gap have separate roles; frozen endpoints supply context.
4. [H3 and motion](identity/04-h3-and-motion.svg): k-ring candidates still need exact motion checks. The schematic grid represents the idea; notebook 1 uses resolution 7 and k=12.
5. [Segments to histories](identity/05-segments-to-histories.svg): a conceptual next step for linking segments across reporting gaps. The notebooks illustrate registry evidence; the cross-segment linking algorithm remains future work.

6. [Receptions to tracks](identity/06-receptions-to-tracks.svg): AIS feed → Bronze receptions → track assignment → Silver positions, including replay from Silver history.
7. [Greedy resolver walkthrough](identity/07-greedy-resolver.html): play, pause or step through notebook-driven assignments before and after late A10. A [looping GIF](identity/07-greedy-resolver.gif) accompanies the interactive version. The figure uses the A/B route subset of notebook 1; jump and near-tie cases are explained in the notebook.

The walkthrough executes notebook 1’s `resolve_group` function to compute its frames. Rebuild it with `python tools/build_resolver_walkthrough.py` from this directory, with pandas, Pillow and `rsvg-convert` installed. `tools/build_track_flow.py` regenerates the flow diagram.

## Port and berth callings

1. [Nested locations](callings/01-port-and-berth.svg): one position can support both a port calling and a berth visit. Port and berth calling histories are computed independently.
2. [Gaps and islands](callings/02-gaps-and-islands.svg): number the complete timeline and the inside matches; their difference separates visits.
3. [Late corrections](callings/03-late-calling-corrections.svg): late A25 splits a visit; A8 moves the first port entry and changes its natural key.

The observation-minute examples follow the notebook fixtures. Spatial layouts and grid cells are schematic. Teal circles represent vessel A, and orange squares represent vessel B where both are shown.
