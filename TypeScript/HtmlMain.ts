class HtmlMain {


    public static layout(): string {
        return '<div id="main"></div>' +
            HtmlStatusBar.layout();
    }

}