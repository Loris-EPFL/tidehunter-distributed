# Working on this distributed Tidehunter fork

Read `docs/distributed/README.md` and the latest dated implementation record before
editing. The October 6 architecture decision is the design anchor; its completion
claims are historical. Recheck source claims against this checkout, based on fresh
fork/upstream main `ce37b18`, rather than the earlier modified local native branch.

For each independent agent task, identify the relevant correctness rule, native
implementation and paper section. Revisit them when changing the ownership, replay,
publication or retention boundary. Cite relevant primary project/paper mechanisms
when adapting external designs; do not copy unrelated transaction machinery solely
because another system uses it. The paper is available in the parent project at
`../middleware/misc/tidehunter.pdf`; design documents link the external sources.

Keep operation identity, logical batch version, current writer authority and
physical address separate. A global logical position is not a durable or published
prefix. Preserve the native backend as a reference and give experimental paths an
explicit identity. Do not claim the partition component serves native Db/Sui until
the real index, replay and adapter paths are wired and tested.

The local machine has one shared SSD and no assumed RDMA hardware. Local tests prove
only the exercised correctness/mechanisms; cloud speed requires matched independent
NVMe measurements later. Avoid large benchmark datasets for protocol unit tests.
