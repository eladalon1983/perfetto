// Copyright (C) 2026 The Android Open Source Project
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import m from 'mithril';
import {Button, ButtonVariant} from '../../widgets/button';
import {Callout} from '../../widgets/callout';
import {Intent} from '../../widgets/common';
import {Popup, PopupPosition} from '../../widgets/popup';
import {TextInput} from '../../widgets/text_input';
import {bigTraceSettingsStorage} from '../settings/bigtrace_settings_storage';
import {
  endpointStorage,
  getBigtraceEndpoint,
} from '../settings/endpoint_storage';

// Host of the configured endpoint, for the button label. Falls back to the raw
// string when it isn't a URL (a half-typed endpoint still reads sensibly).
function endpointLabel(endpoint: string): string {
  if (endpoint === '') return 'Connect backend';
  try {
    return new URL(endpoint).host;
  } catch {
    return endpoint;
  }
}

function formatBytes(bytes: number | undefined): string {
  if (bytes === undefined || bytes <= 0) return '0 B';
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KiB`;
  if (bytes < 1024 * 1024 * 1024) {
    return `${(bytes / (1024 * 1024)).toFixed(1)} MiB`;
  }
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} GiB`;
}

interface TpdStatus {
  enabled: boolean;
  profile?: string;
  tracesKnown?: number;
  hotCount?: number;
  warmCount?: number;
  coldCount?: number;
  emptyCount?: number;
  craStoreSnapshots?: number;
  craStoreBytes?: number;
  craCaptureCount?: number;
  craRestoreCount?: number;
  residentBytes?: number;
  memoryBudgetBytes?: number;
}

// The endpoint is the one piece of state that isn't per-query — it's a
// connection, not a configuration — so it lives up here rather than in the
// per-tab settings. Auto-opens once when there's no working backend, which is
// the app's onboarding step now that the settings page is gone.
export class ConnectionButton implements m.ClassComponent {
  private open = false;
  private autoOpened = false;
  private tpdStatus?: TpdStatus;
  private tpdLoading = false;
  private pollTimer?: ReturnType<typeof setInterval>;

  oninit() {
    this.fetchTpdStatus();
    this.pollTimer = setInterval(() => {
      if (this.open) {
        this.fetchTpdStatus();
      }
    }, 2000);
  }

  onremove() {
    if (this.pollTimer !== undefined) {
      clearInterval(this.pollTimer);
    }
  }

  private async fetchTpdStatus() {
    const endpoint = getBigtraceEndpoint();
    if (!endpoint || typeof fetch !== 'function') return;
    try {
      const resp = await fetch(`${endpoint}/tpd/status`, {mode: 'cors'});
      if (resp.ok) {
        this.tpdStatus = (await resp.json()) as TpdStatus;
        m.redraw();
      }
    } catch {
      // Ignore if backend doesn't support /tpd/status (e.g. cloud backend).
    }
  }

  private async setTpdProfile(profile: 'batch' | 'interactive') {
    const endpoint = getBigtraceEndpoint();
    if (!endpoint || this.tpdLoading) return;
    this.tpdLoading = true;
    m.redraw();
    try {
      const resp = await fetch(`${endpoint}/tpd/config`, {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({profile}),
        mode: 'cors',
      });
      if (resp.ok) {
        this.tpdStatus = (await resp.json()) as TpdStatus;
      }
    } finally {
      this.tpdLoading = false;
      m.redraw();
    }
  }

  private async resetTpdSession() {
    const endpoint = getBigtraceEndpoint();
    if (!endpoint || this.tpdLoading) return;
    this.tpdLoading = true;
    m.redraw();
    try {
      const resp = await fetch(`${endpoint}/tpd/reset_session`, {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: '{}',
        mode: 'cors',
      });
      if (resp.ok) {
        const data = (await resp.json()) as {status?: TpdStatus};
        if (data.status) this.tpdStatus = data.status;
      }
    } finally {
      this.tpdLoading = false;
      m.redraw();
    }
  }

  view(): m.Children {
    const endpoint = getBigtraceEndpoint();
    // The endpoint ships with a default, so "set" doesn't mean "reachable" —
    // a backend that failed to answer counts as not connected, otherwise this
    // button claims a connection the app doesn't have.
    const connected =
      endpoint !== '' &&
      bigTraceSettingsStorage.execConfigLoadError === undefined;
    if (
      !this.autoOpened &&
      !connected &&
      !bigTraceSettingsStorage.isExecConfigLoading
    ) {
      this.autoOpened = true;
      this.open = true;
    }
    const tpd = this.tpdStatus;
    const baseLabel = endpointLabel(endpoint);
    const label =
      connected && tpd?.enabled
        ? `${baseLabel} · tpd (${tpd.profile ?? 'batch'})`
        : baseLabel;
    return m(
      Popup,
      {
        trigger: m(Button, {
          icon: connected ? 'cloud_done' : 'cloud_off',
          label,
          title: connected
            ? endpoint
            : `Not connected to ${endpoint === '' ? 'a backend' : endpoint}`,
          onclick: () => {
            this.open = !this.open;
            if (this.open) this.fetchTpdStatus();
          },
        }),
        isOpen: this.open,
        onChange: (isOpen: boolean) => {
          this.open = isOpen;
          if (isOpen) this.fetchTpdStatus();
        },
        position: PopupPosition.BottomEnd,
        className: 'pf-bt-connection-popup',
      },
      this.renderPanel(),
    );
  }

  private renderPanel(): m.Children {
    const setting = endpointStorage.get('bigtraceEndpoint');
    if (setting === undefined) return null;
    const loadError = bigTraceSettingsStorage.execConfigLoadError;
    const tpd = this.tpdStatus;
    return m('.pf-bt-connection-panel', [
      m('.pf-bt-connection-panel__title', 'BigTrace backend'),
      m(
        '.pf-bt-connection-panel__desc',
        'The service that holds the traces and runs your queries.',
      ),
      m(TextInput, {
        value: setting.get() as string,
        placeholder: 'https://your-bigtrace-backend/v1',
        className: 'pf-bt-connection-panel__input',
        oninput: (e: Event) => {
          setting.set((e.target as HTMLInputElement).value);
        },
      }),
      m('.pf-bt-connection-panel__actions', [
        // The endpoint is read once at module init, so a change needs a reload.
        endpointStorage.isReloadRequired() &&
          m(Button, {
            label: 'Reload to apply',
            icon: 'refresh',
            intent: Intent.Primary,
            variant: ButtonVariant.Filled,
            onclick: () => window.location.reload(),
          }),
        m(Button, {
          label: 'Reset',
          icon: 'settings_backup_restore',
          title: 'Restore the default endpoint.',
          onclick: () => setting.reset(),
        }),
      ]),
      loadError !== undefined &&
        m(
          Callout,
          {intent: Intent.Danger, icon: 'error'},
          `Couldn't load settings from this backend: ${loadError}`,
        ),
      tpd?.enabled && [
        m('.pf-bt-tpd-section', [
          m('.pf-bt-connection-panel__title', 'tpd Pool Engine'),
          m(
            '.pf-bt-connection-panel__desc',
            `Known: ${tpd.tracesKnown ?? 0} · Hot: ${tpd.hotCount ?? 0} · Warm: ${tpd.warmCount ?? 0} · Cold (CRA): ${tpd.coldCount ?? 0} · Empty: ${tpd.emptyCount ?? 0}`,
          ),
          m(
            '.pf-bt-connection-panel__desc',
            `CRA Snapshots: ${tpd.craStoreSnapshots ?? 0} (${formatBytes(tpd.craStoreBytes)}) · Captures: ${tpd.craCaptureCount ?? 0} · Restores: ${tpd.craRestoreCount ?? 0}`,
          ),
          m('.pf-bt-connection-panel__actions', [
            m(Button, {
              label: 'Batch mode',
              icon: 'bolt',
              disabled: this.tpdLoading,
              intent: tpd.profile === 'batch' ? Intent.Primary : Intent.None,
              variant:
                tpd.profile === 'batch'
                  ? ButtonVariant.Filled
                  : ButtonVariant.Minimal,
              title:
                'High-throughput sweep mode: releases each TP to Cold after its query.',
              onclick: () => this.setTpdProfile('batch'),
            }),
            m(Button, {
              label: 'Interactive mode',
              icon: 'terminal',
              disabled: this.tpdLoading,
              intent:
                tpd.profile === 'interactive' ? Intent.Primary : Intent.None,
              variant:
                tpd.profile === 'interactive'
                  ? ButtonVariant.Filled
                  : ButtonVariant.Minimal,
              title:
                'Stateful mode: keeps Hot TPs resident and replays session SQL across evictions.',
              onclick: () => this.setTpdProfile('interactive'),
            }),
            m(Button, {
              label: 'Reset session',
              icon: 'restart_alt',
              disabled: this.tpdLoading,
              title: 'Evict Hot TPs to Cold and clear interactive SQL replay.',
              onclick: () => this.resetTpdSession(),
            }),
          ]),
        ]),
      ],
    ]);
  }
}
