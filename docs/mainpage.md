pushseg {#mainpage}
==

Segment-based, push-only data transfer between targets over pluggable
transports. See the top-level `README.md` for the memory contract and how to
plug in a backend.

- Application API: pushseg::Context, pushseg::LocalSegment, pushseg::Connection, pushseg::load
- Backend SPI: pushseg::backend::StreamBackend, pushseg::backend::MappedBackend

@code
#include <pushseg/pushseg.hpp>

pushseg::Config cfg = pushseg::Config::load("system.conf");
pushseg::Context ctx(cfg, 1, "algo");
pushseg::Connection c = ctx.connect(0, "algo", 0, 1);
c.write_notify(0, data, len, 42);
@endcode
