USER_UNIT_DIR := $(or $(XDG_CONFIG_HOME),$(HOME)/.config)/systemd/user

.PHONY: install
install:
	cargo install --locked --path . --bin ghs --root "$(HOME)/.local"
	install -Dm644 systemd/ghs.service "$(USER_UNIT_DIR)/ghs.service"
