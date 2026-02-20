import * as React from 'react';
import * as ReactDom from 'react-dom';
import { Version } from '@microsoft/sp-core-library';
import {
  type IPropertyPaneConfiguration,
  PropertyPaneTextField,
  PropertyPaneToggle
} from '@microsoft/sp-property-pane';
import { BaseClientSideWebPart } from '@microsoft/sp-webpart-base';
import { IReadonlyTheme } from '@microsoft/sp-component-base';

import * as strings from 'NonBudgetedConferenceFormWebPartStrings';
import NonBudgetedConferenceForm from './components/NonBudgetedConferenceForm';
import { INonBudgetedConferenceFormProps } from './components/INonBudgetedConferenceFormProps';

export interface INonBudgetedConferenceFormWebPartProps {
  orgDevApprovers: string;
  accountingApprovers: string;
  listName: string;
  enableEmailNotifications: boolean;
}

export default class NonBudgetedConferenceFormWebPart extends BaseClientSideWebPart<INonBudgetedConferenceFormWebPartProps> {

  private _isDarkTheme: boolean = false;
  private _environmentMessage: string = '';

  public render(): void {
    const element: React.ReactElement<INonBudgetedConferenceFormProps> = React.createElement(
      NonBudgetedConferenceForm,
      {
        context: this.context,
        orgDevApprovers: this.properties.orgDevApprovers,
        accountingApprovers: this.properties.accountingApprovers,
        listName: this.properties.listName || 'NonBudgetedConferenceRequests'
      }
    );

    ReactDom.render(element, this.domElement);
  }

  protected onInit(): Promise<void> {
    return this._getEnvironmentMessage().then(message => {
      this._environmentMessage = message;
    });
  }



  private _getEnvironmentMessage(): Promise<string> {
    if (!!this.context.sdks.microsoftTeams) { // running in Teams, office.com or Outlook
      return this.context.sdks.microsoftTeams.teamsJs.app.getContext()
        .then(context => {
          let environmentMessage: string = '';
          switch (context.app.host.name) {
            case 'Office': // running in Office
              environmentMessage = this.context.isServedFromLocalhost ? strings.AppLocalEnvironmentOffice : strings.AppOfficeEnvironment;
              break;
            case 'Outlook': // running in Outlook
              environmentMessage = this.context.isServedFromLocalhost ? strings.AppLocalEnvironmentOutlook : strings.AppOutlookEnvironment;
              break;
            case 'Teams': // running in Teams
            case 'TeamsModern':
              environmentMessage = this.context.isServedFromLocalhost ? strings.AppLocalEnvironmentTeams : strings.AppTeamsTabEnvironment;
              break;
            default:
              environmentMessage = strings.UnknownEnvironment;
          }

          return environmentMessage;
        });
    }

    return Promise.resolve(this.context.isServedFromLocalhost ? strings.AppLocalEnvironmentSharePoint : strings.AppSharePointEnvironment);
  }

  protected onThemeChanged(currentTheme: IReadonlyTheme | undefined): void {
    if (!currentTheme) {
      return;
    }

    this._isDarkTheme = !!currentTheme.isInverted;
    const {
      semanticColors
    } = currentTheme;

    if (semanticColors) {
      this.domElement.style.setProperty('--bodyText', semanticColors.bodyText || null);
      this.domElement.style.setProperty('--link', semanticColors.link || null);
      this.domElement.style.setProperty('--linkHovered', semanticColors.linkHovered || null);
    }

  }

  protected onDispose(): void {
    ReactDom.unmountComponentAtNode(this.domElement);
  }

  protected get dataVersion(): Version {
    return Version.parse('1.0');
  }

  protected getPropertyPaneConfiguration(): IPropertyPaneConfiguration {
    return {
      pages: [
        {
          header: {
            description: strings.PropertyPaneDescription
          },
          groups: [
            {
              groupName: strings.BasicGroupName,
              groupFields: [
                PropertyPaneTextField('listName', {
                  label: 'SharePoint List Name',
                  value: 'NonBudgetedConferenceRequests'
                }),
                PropertyPaneTextField('orgDevApprovers', {
                  label: 'Org Dev Approvers (semicolon-delimited emails)',
                  multiline: true
                }),
                PropertyPaneTextField('accountingApprovers', {
                  label: 'Accounting Approvers (semicolon-delimited emails)',
                  multiline: true
                }),
                PropertyPaneToggle('enableEmailNotifications', {
                  label: 'Enable Email Notifications'
                })
              ]
            }
          ]
        }
      ]
    };
  }
}
