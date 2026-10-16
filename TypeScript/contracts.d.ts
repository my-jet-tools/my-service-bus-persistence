
interface ITopicInfo {
    topicId: string;
    messageId: number;
    lastSaveDur: string;
    lastSaveMoment: string;
    loadedPages: ILoadedPage[];
    activePages: number[];
    queues: ITopicQueue[];
}

interface ILoadedPage {
    pageId: number,
    subPages: number[],
    count: number,
    size: number
}

interface ITopicQueue {
    queueId: string;
    ranges: IQueueRange[];
}

interface IQueueRange {
    fromId: number;
    toId: number;
}

interface IPersistentOperation {
    name: string;
    topicId: string;
    pageId: number,
    dur: string
}

interface IStatus {
    topics: ITopicInfo[];
    awaitingOperations: IPersistentOperation[];
    queuesSnapshotId: number;
    activeOperations: IPersistentOperation[];
    system: ISystemStatus,
    initialing: boolean
}


interface ISystemStatus {
    usedmem: number,
    totalmem: number
}