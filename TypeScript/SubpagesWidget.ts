class SubPagesWidget {


    public static renderPagesWidget(subPages: number[]): string {

        let result = '<div class="page-widget"><svg style="font-size:16px" width="400" height="20">' +
            '<rect width="400" height="20" rx="5" ry="5" style="fill:white;stroke-width:;stroke:black"/>';



        for (let i of subPages) {
            result +=
                '<line x1="' +
                (i * 4 + 2) +
                '" y1="' +
                0 +
                '" x2="' +
                (i * 4 + 2) +
                '" y2="' +
                20 +
                '" style="stroke:blue;stroke-width:2" />';
        }


        result += '</svg></div>';

        return result;

    }

}