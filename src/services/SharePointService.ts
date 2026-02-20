import { WebPartContext } from '@microsoft/sp-webpart-base';
import { SPHttpClient, SPHttpClientResponse, ISPHttpClientOptions } from '@microsoft/sp-http';
import { IConferenceRequest, RequestStatus } from '../models/IConferenceRequest';

export class SharePointService {
    private context: WebPartContext;
    private listName: string;

    constructor(context: WebPartContext, listName: string) {
        this.context = context;
        this.listName = listName;
    }

    private getListUrl(): string {
        return `${this.context.pageContext.web.absoluteUrl}/_api/web/lists/getByTitle('${this.listName}')`;
    }

    public async getRequests(filter?: string): Promise<IConferenceRequest[]> {
        let url = `${this.getListUrl()}/items?$top=5000`;
        if (filter) {
            url += `&$filter=${filter}`;
        }

        try {
            const response: SPHttpClientResponse = await this.context.spHttpClient.get(url, SPHttpClient.configurations.v1);
            if (response.ok) {
                const json = await response.json();
                return json.value as IConferenceRequest[];
            }
            throw new Error(await response.text());
        } catch (error) {
            console.error('Error fetching requests', error);
            throw error;
        }
    }

    public async getRequestById(id: number): Promise<IConferenceRequest> {
        const url = `${this.getListUrl()}/items(${id})`;
        try {
            const response: SPHttpClientResponse = await this.context.spHttpClient.get(url, SPHttpClient.configurations.v1);
            if (response.ok) {
                return await response.json() as IConferenceRequest;
            }
            throw new Error(await response.text());
        } catch (error) {
            console.error(`Error fetching request ${id}`, error);
            throw error;
        }
    }

    public async createRequest(request: Partial<IConferenceRequest>): Promise<IConferenceRequest> {
        const url = `${this.getListUrl()}/items`;

        // Auto-calculate TotalEstimatedBudget if not provided or to ensure accuracy
        request.TotalEstimatedBudget = this.calculateTotal(request);

        const options: ISPHttpClientOptions = {
            body: JSON.stringify(request)
        };

        try {
            const response: SPHttpClientResponse = await this.context.spHttpClient.post(url, SPHttpClient.configurations.v1, options);
            if (response.ok) {
                return await response.json() as IConferenceRequest;
            }
            throw new Error(await response.text());
        } catch (error) {
            console.error('Error creating request', error);
            throw error;
        }
    }

    public async updateRequest(id: number, request: Partial<IConferenceRequest>): Promise<void> {
        const url = `${this.getListUrl()}/items(${id})`;

        if (request.RegistrationCost !== undefined) {
            // if costs are being updated, recalculate total
            request.TotalEstimatedBudget = this.calculateTotal(request);
        }

        const options: ISPHttpClientOptions = {
            headers: {
                'IF-MATCH': '*',
                'X-HTTP-Method': 'MERGE'
            },
            body: JSON.stringify(request)
        };

        try {
            const response: SPHttpClientResponse = await this.context.spHttpClient.post(url, SPHttpClient.configurations.v1, options);
            if (!response.ok) {
                throw new Error(await response.text());
            }
        } catch (error) {
            console.error(`Error updating request ${id}`, error);
            throw error;
        }
    }

    private calculateTotal(req: Partial<IConferenceRequest>): number {
        return (req.RegistrationCost || 0) +
            (req.AirfareCost || 0) +
            (req.LodgingCost || 0) +
            (req.MeetingRoomRentalCost || 0) +
            (req.CarRentalCost || 0) +
            (req.TravelMealAllowanceCost || 0) +
            (req.ConferenceMealsCost || 0) +
            (req.OtherCost || 0);
    }
}
