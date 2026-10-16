
class HtmlStatusBar {

    private static queueSnapshotId: number;

    private static connected: boolean;
    private static initializing: boolean;

    public static layout(): string {
        return '<div id="status-bar">' +
            '<table><tr>' +

            '<td style="padding-left: 5px">Connected: <b id="connected" style="text-shadow: 0 0 2px white;"></b></td>' +
            '<td><div class="statusbar-separator"></div></td>' +

            '<td style="padding-left: 5px; min-width:250px">Queue snapshotId: <b id="snapshot-id" style="text-shadow: 0 0 2px white;"></b></td>' +
            '<td><div class="statusbar-separator"></div></td>' +

            '<td style="padding-left: 5px; min-width:200px; font-size:12px;">Total pages size:<span id="total-pages-size" style="text-shadow: 0 0 2px white;"></span></td>' +
            '<td><div class="statusbar-separator"></div></td>' +

            '<td style="padding-left: 5px; min-width:270px"><span id="cpu-mem" style="text-shadow: 0 0 2px white;"></span></td>' +
            '<td><div class="statusbar-separator"></div></td>';
    }



    public static updateStatusbar(data: IStatus) {

        if (this.queueSnapshotId != data.queuesSnapshotId) {
            this.queueSnapshotId = data.queuesSnapshotId;
            document.getElementById('snapshot-id').innerHTML = data.queuesSnapshotId.toString();
        }

        if (!this.connected || this.initializing != data.initialing) {
            this.connected = true;
            this.initializing = data.initialing;

            if (data.initialing) {
                document.getElementById('connected').innerHTML = '<span style="color: yellow">initializing</span>';
            }
            else {
                document.getElementById('connected').innerHTML = '<span style="color: green">online</span>';
            }

        }

        document.getElementById('cpu-mem').innerHTML = 'Mem: <span>' + HtmlRenderer.formatMem(data.system.usedmem * 1024) + ' of ' + HtmlRenderer.formatMem(data.system.totalmem * 1024) + '</span>';

    }

    public static updateOffline() {
        if (this.connected) {
            this.connected = false;
            document.getElementById('connected').innerHTML = '<span style="color: red">offline</span>';
        }
    }
}