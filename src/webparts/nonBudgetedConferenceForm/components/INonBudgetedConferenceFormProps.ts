import { WebPartContext } from '@microsoft/sp-webpart-base';

export interface INonBudgetedConferenceFormProps {
  context: WebPartContext;
  orgDevApprovers: string;
  accountingApprovers: string;
  listName: string;
}
