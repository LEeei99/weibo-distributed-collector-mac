# Source and compatibility

Derived from LEeei99/weibo-distributed-collector at commit 993f5558385d0180acb42bd77dbf9c61dcf2aec7.

This repository targets macOS workers. Package, assignment and return schemas remain unchanged. The controller keeps the single authoritative dispatch.sqlite; this repository never creates a second live dispatch registry. Coordinator tools are retained for offline compatibility tests. No production package, assignment, credential, checkpoint or media is included.

Mac workers must start with separately signed packages and fresh local worker roots. Do not move an active Windows worker root to a Mac. The source manifest records the Mac adaptation as a distinct code version.
