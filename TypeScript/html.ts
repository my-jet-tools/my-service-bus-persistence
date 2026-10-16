

class HtmlRenderer {



    public static formatNumber(n: number): string {

        return n.toString().replace(/(\d)(?=(\d{3})+(?!\d))/g, '$1 ');
    }

    public static formatMem(n: number): string {

        if (n < 1024) {
            return n.toFixed(2) + "b";
        }

        n = n / 1024;

        if (n < 1024) {
            return n.toFixed(2) + "Kb";
        }

        n = n / 1024;

        if (n < 1024) {
            return n.toFixed(2) + "Mb";
        }

        n = n / 1024;
        return n.toFixed(2) + "Gb";

    }


    private static renderQueuesTableContent(queues: ITopicQueue[]): string {
        let result = '';

        for (let queue of queues) {
            result += '<div><b>' + queue.queueId + '</b></div>';

            for (let range of queue.ranges) {
                result += '<div style="margin-left: 10px">' + range.fromId + ' - ' + range.toId + '</div>';
            }

            result += '<hr/>';
        }

        return result;
    }


    private static compileTable(values: string[]): string {

        let result = '<table style="width: 100%"><tr>';

        for (let value of values) {
            result += '<td>' + value + '</td>';
        }

        return result + '</tr></table>';

    }


    private static renderLoadedPagesContent(topics: ITopicInfo[]): string {
        let result = '';

        let totalPagesSize = 0;

        for (let topic of topics.sort((a, b) => {
            return a.topicId > b.topicId ? 1 : -1
        })) {


            let activePagesBadges = '';
            for (let activePage of topic.activePages) {
                activePagesBadges += '<span class="badge badge-warning" style="margin-left: 5px">' + activePage + '</span>';
            }

            let queuesContent = this.renderQueuesTableContent(topic.queues);


            for (let page of topic.loadedPages) {
                totalPagesSize += page.size;
            }

            result += '<tr style="font-size: 12px">' +
                '<td>' + topic.topicId +
                '<div>Active:</div>' + activePagesBadges + '<hr/><div>Loaded:</div>' + this.renderCachedPages(topic.loadedPages) + '</td>' +
                '<td>' + queuesContent + '</td>' +
                '<td><div>Current Id:' + topic.messageId + '</div>' +
                '<div>Last Save Duration:' + topic.lastSaveDur + '</div>' +
                '<div>Saved ago:' + topic.lastSaveMoment + '</div>' +

                '</td>' +
                '</tr>'
        }


        document.getElementById('total-pages-size').innerHTML = HtmlRenderer.formatNumber(totalPagesSize);


        return result;
    }

    public static renderMainTable(topics: ITopicInfo[]): string {

        let content = this.renderLoadedPagesContent(topics);

        return '<table class="table table-striped"><tr><th>Topic</th><th>Queues</th><th>MessageId</th></tr>' + content + '</table>';

    }


    public static renderMainContent(r: IStatus): string {

        if (r.initialing) {
            return '<h1 style="color:red">Application is being initialized</h1><div style="color:gray">' + this.renderMainTable(r.topics) + '</div>';
        }

        return this.renderMainTable(r.topics);

    }

    private static renderCachedPages(pages: ILoadedPage[]) {
        let result = "";

        for (let page of pages) {
            result +=
                '<div><div>Page:' + page.pageId + '; Amount:' + page.count + '; Size: ' + this.formatNumber(page.size) + '</div>' +
                SubPagesWidget.renderPagesWidget(page.subPages) +
                '</div>';
        }

        return result;
    }




}