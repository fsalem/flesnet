CBM FLES Timeslice Building
===========================
[![Build Status](https://travis-ci.org/fsalem/flesnet.svg?branch=fles_libfabric_DFS)](https://travis-ci.org/github/fsalem/flesnet)
[![Coverage Status](https://coveralls.io/repos/github/fsalem/flesnet/badge.svg?branch=fles_libfabric_DFS)](https://coveralls.io/github/fsalem/flesnet?branch=fles_libfabric_DFS)

The First-Level Event Selector (FLES) system of the CBM experiment employs a
scheme of timeslices (consisting of microslices) instead of events in data
aquisition. This software aims to implement the timeslice building process
between several nodes of the FLES cluster over an Infiniband network. It is
also used for testbeam readout using the FLES Interface Board (FLIB).


Getting started
---------------

If you are using *flesnet* for the first time or setting up a FLIB-based
readout chain, please read [`INSTALL.md`](INSTALL.md) and
[`HOWTO.md`](HOWTO.md) in this repository. These files will be continuously
updated, so it might be worth checking them from time to time. Additional
information on how to set up a system for flesnet can be found in the
[flesnet wiki][flesnet-wiki].

CBM scientists: For updates and information concerning *flesnet* and the
operation of a readout chain, please join the
[cbm-flibuser mailing list][flibuser-list] via [this form][list-membership].

[flesnet-wiki]: https://github.com/cbm-fles/flesnet/wiki
[flibuser-list]: https://www-cbm.gsi.de/cbmcdb/display.cgi?obj=grup;view=show;gid=31
[list-membership]: https://www-cbm.gsi.de/cbmcdb/modpers.cgi?view=grup
