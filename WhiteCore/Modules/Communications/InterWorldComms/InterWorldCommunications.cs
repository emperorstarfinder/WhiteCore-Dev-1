/*
 * Copyright (c) Contributors, http://WhiteCore-sim.org/
 * See CONTRIBUTORS.TXT for a full list of copyright holders.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the WhiteCore-Sim Project nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE DEVELOPERS ``AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE CONTRIBUTORS BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Nini.Config;
using OpenMetaverse;
using OpenMetaverse.StructuredData;
using WhiteCore.Framework.Servers.HttpServer;
using WhiteCore.Framework.Services;
using WhiteCore.Framework.Modules;
using WhiteCore.Framework.ConsoleFramework;
using WhiteCore.Framework.Utilities;
using WhiteCore.Framework.Servers;
using WhiteCore.Framework.Servers.HttpServer.Implementation;
using WhiteCore.Framework.SceneInfo;
using GridRegion = WhiteCore.Framework.Services.GridRegion;
using RegionFlags = WhiteCore.Framework.Services.RegionFlags;


namespace WhiteCore.Modules.Communications
{
    public class InterWorldCommunications : IService, IWCCommunicationService
    {

        #region Declares

		/// <summary>
        ///   All connections that we have to other hosts
        ///   (Before sending the initial connection requests, 
        ///   this MAY contain connections that we do not currently have)
        /// </summary>
        protected List<string> Connections = new List<string>();

        /// <summary>
        ///   The 'WhiteCoreInterWorldConnectors' config
        /// </summary>
        protected IConfig m_config;
        protected ThreatLevel m_untrustedConnectionsDefaultTrust = ThreatLevel.Low;

        /// <summary>
        ///   The registry where we can get services
        /// </summary>
        protected IRegistryCore m_registry;

        public bool IsGettingUrlsForIWCConnection
        {
            get;
            set;
        }

		/// <summary>
		///   The class that sends requests to other hosts
		/// </summary>
		public IWCIncomingConnections IncomingIWCComms;

		 /// <summary>
        ///   The class that sends requests to other hosts
        /// </summary>
        public IWCOutgoingConnections OutgoingIWCComms;

        /// <summary>
        ///   Whether we are enabled or not
        /// </summary>
        public bool Enabled
        {
            get;
            set;
        }

        /// <summary>
        ///   Should connections that come to us that are not authenticated be allowed to connect?
        /// </summary>
        public bool AllowUntrustedConnections
        {
            get;
            set;
        }

        /// <summary>
        ///   Untrusted connections automatically get this trust level
        /// </summary>
        public ThreatLevel UntrustedConnectionsDefaultTrust 
        { 
            get { return m_untrustedConnectionsDefaultTrust; } 
            set { m_untrustedConnectionsDefaultTrust = value; } 
        }


        // accss for incomming & outgoing comms 
        public IRegistryCore Registry
        {
            get { return m_registry; }
        }

        public string GridName
        {
            get { return m_config.GetString("gridname", String.Empty); }
        }

        #endregion

		#region IService Members 

		public string Name
		{
			get { return "InterWorldCommunications"; }
		}

		public void Initialize(IConfigSource source, IRegistryCore registry)
		{
			m_config = source.Configs["InterWorldConnectors"];
			if (m_config != null)
			{
				Enabled = m_config.GetBoolean("Enabled", false);
				AllowUntrustedConnections = m_config.GetBoolean("AllowUntrustedConnections",
				                                                  AllowUntrustedConnections);
				m_untrustedConnectionsDefaultTrust =
					(ThreatLevel)
						Enum.Parse(typeof (ThreatLevel),
						           m_config.GetString("UntrustedConnectionsDefaultTrust",
						                   m_untrustedConnectionsDefaultTrust.ToString()));

				registry.RegisterModuleInterface<InterWorldCommunications>(this);
//				Init(registry, Name);
//				registry.RegisterModuleInterface(this);
                registry.StackModuleInterface<IWCCommunicationService>(this);
				m_registry = registry;
			}
		}

		public void Start(IConfigSource source, IRegistryCore registry)
		{
		}

		public void FinishedStartup()
		{
			if (Enabled)
			{
				//Set up the public connection
				IncomingIWCComms = new IWCIncomingConnections (this);
				//Startup outgoing
				OutgoingIWCComms = new IWCOutgoingConnections(this);

				//Make our connection strings.
                //Connections = BuildConnections();

				try
				{
					ContactOtherServers();
				}
				catch
				{
				}

				AddConsoleCommands();
			}
		}

		#endregion

        #region helpers

        /// <summary>
        /// Add the local grid service URIs.
        /// </summary>
        public OSDMap GetLocalGridURIs()
        {
            OSDMap response = new OSDMap();

            //Add our URL for confirmation
            response ["OurIdentifier"] = Utilities.GetAddress ();
            response ["GridName"] = GridName;
            response ["Success"] = false;

            //Add our grid service URIs 
            IGridInfo gridInfo = m_registry.RequestModuleInterface<IGridInfo>();
            if (gridInfo != null)
            {
                var localInfo = gridInfo.GetGridInfoHashtable();
                foreach (string key in localInfo.Keys)
                    response.Add (key, (string)localInfo [key]);
//                    response [key] = (OSD)localInfo [key];
                response ["Success"] = true;
            } 
            return response;
        }

        /// <summary>
        /// Gets the local regions for an IWC call.
        /// </summary>
        /// <returns>The local regions.</returns>
        public OSDMap GetLocalRegions()
        {
            OSDMap response = new OSDMap();

            //Add our URL for confirmation
            response ["OurIdentifier"] = Utilities.GetAddress ();
            response ["GridName"] = GridName;
            response ["Success"] = false;

            IGridService service = m_registry.RequestModuleInterface<IGridService>();
            if (service != null)
            {
                var ids = new List <UUID> ();
                ids.Add (UUID.Zero);
                List<GridRegion> regions = service.GetRegionsByName (null, "#", null, null);
                int idx = 1;
                string curRegion;
                foreach (GridRegion gr in regions)
                {
                    curRegion = "Region " + idx;
                    response [curRegion] = gr.RegionName;
                    response [curRegion + ":X"] = gr.RegionLocX / Constants.RegionSize;
                    response [curRegion + ":Y"] = gr.RegionLocY / Constants.RegionSize;
                    response [curRegion + ":Size"] = gr.RegionSizeX + "x" + gr.RegionSizeY;
                    response [curRegion + ":URI"] = gr.RegionURI;
                }

                response ["Success"] = true;

            } 
            return response;
        }

        /// <summary>
        /// Add the local region URIs.
        /// </summary>
        /// <param name="regUris">Reg uris.</param>
        public OSDMap GetLocalRegionURIs( OSDMap regUris)
        {
            if (regUris == null)
                return null;

            //Add our region URIs 
            IGridService service = m_registry.RequestModuleInterface<IGridService>();
            if (service != null)
            {
                List<UUID> ids = new List <UUID> ();
                ids.Add (UUID.Zero);
                List<GridRegion> regions = service.GetRegionsByName (ids, null, 0, 10);
                int idx = 1;
                string curRegion;
                foreach (GridRegion gr in regions)
                {
                    curRegion = "Region " + idx;
                    regUris [curRegion] = gr.RegionName;
                    regUris [curRegion + ":URI"] = gr.RegionURI;
                }
            } 
            return regUris;
        }


        /// <summary>
        /// Gets the IWC region details.
        /// </summary>
        /// <returns>The IWC region.</returns>
        /// <param name="regionID">Region I.</param>
        public OSDMap GetIWCRegion(string regionID)
        {
            OSDMap response = new OSDMap();
            response ["OurIdentifier"] = Utilities.GetAddress ();
            response ["GridName"] = GridName;
            response ["Success"] = false;

            IGridService service = m_registry.RequestModuleInterface<IGridService>();
            if (service != null)
            {
                List<UUID> ids = new List <UUID> ();
                ids.Add (UUID.Zero);

                var regionUUID = (UUID)regionID;
                var region = service.GetRegionByUUID (null, regionUUID);

                if (region != null)
                {
                    response ["RegionID"] = region.RegionID;
                    response ["RegionURI"] = region.RegionURI;
                    response ["RegionHandle"] = region.RegionHandle;
                    response ["RegionName"] = region.RegionName;
                    response ["LocX"] = region.RegionLocX;
                    response ["LocX"] = region.RegionLocX;
                    response ["LocX"] = region.RegionLocX;
                    response ["SizeX"] = region.RegionSizeX;
                    response ["SizeY"] = region.RegionSizeY;
                    response ["MapTile"] = region.TerrainMapImage;

                    response ["Success"] = true;

                }
            }

            return response;

        }

        #endregion

		#region IWCCommunicationService Members
        /// <summary>
        /// Gets the region for the grid url.
        /// </summary>
        /// <returns>The region for grid.</returns>
        /// <param name="regionName">Region name.</param>
        /// <param name="url">URL of the grid to check.</param>
		public GridRegion GetRegionForGrid(string regionName, string url)
		{
			bool found = Connections.Contains(url);
			if (found)
			{
				//If we are already connected, the grid services are together, so we already know of the region if it exists, therefore, it does not exist
				return null;
			}
			else
			{
                url = CheckIwcURL (url);

				bool success = OutgoingIWCComms.AttemptConnection(url);
				if (success)
				{
					IGridService service = m_registry.RequestModuleInterface<IGridService>();
					if (service != null)
					{
                        var ids = new List <UUID> ();
						ids.Add (UUID.Zero);
						List<GridRegion> regions = service.GetRegionsByName(ids, regionName, 0,3);

						foreach (GridRegion t in regions)
						{
							if (t.RegionName == regionName)
							{
								return t;
							}
						}
						if (regions.Count > 0)
							return regions[0];
					}
				}
			}
			return null;
		}

        /// <summary>
        /// Gets the urls for IWC region user.
        /// </summary>
        /// <returns>The urls for user.</returns>
        /// <param name="region">Region.</param>
        /// <param name="userID">User ID</param>
        public OSDMap GetUrlsForUser(GridRegion region, UUID userID)
		{
			if ((((RegionFlags) region.Flags) & RegionFlags.Foreign) != RegionFlags.Foreign)
			{
				MainConsole.Instance.Debug("[IWC]: Not a foreign region");
				return null;
			}
			string host = userID.ToString();
            IGridRegistrationService module = m_registry.RequestModuleInterface<IGridRegistrationService>();
			if (module != null)
			{
				module.RemoveUrlsForClient(host);
				module.RemoveUrlsForClient(host + "|" + region.RegionHandle);
				IsGettingUrlsForIWCConnection = true;
				OSDMap map = module.GetUrlForRegisteringClient(host + "|" + region.RegionHandle);
				IsGettingUrlsForIWCConnection = false;

				string url = region.GenericMap["URL"];
				if (url == "")
				{
					MainConsole.Instance.Warn("[IWC]: Foreign region with no URL");
					return null; //What the hell? Its a foreign region, it better have a URL!
				}
				//Remove the /Grid.... stuff
				url = url.Remove(url.Length - 5 - 36);
				OutgoingIWCComms.InformOfURLs(url + "/iwcconnection", map, userID, region.RegionHandle);

				return map;
			}

			return null;
		}


		#endregion

		#region Console Commands

		/// <summary>
		/// Command line interface help.
		/// </summary>
		/// <param name="scene">Scene.</param>
		/// <param name="cmd">Cmd.</param>
		void InterfaceHelp(string[] cmd)
		{
			if (!MainConsole.Instance.HasProcessedCurrentCommand)
				MainConsole.Instance.HasProcessedCurrentCommand = true;

			MainConsole.Instance.Info(
                "iwc connect [<Remote host>:<port>]"+
				"\n   Create an IWC connection to another WhiteCore host"+
                "\n   [<Remote host>:<port>] - Optional, if not supplied you will be prompted for the host URI to connect."
				);
			MainConsole.Instance.Info(
                "iwc disconnect [<Remote host>:<port>]"+
                "\n   disconnect an established IWC connection to another WhiteCore host"+
                "\n   [<Remote host>:<port>] - Optional, if not supplied you will be prompted for the host URI to disconnect.");
			MainConsole.Instance.Info(
				"iwc show"+ 
			    "\n   Show all active IWC connections.");
            MainConsole.Instance.Info(
                "iwc host regions [<Remote host>:<port>]"+
                "\n   Show region details of an IWC host"+
                "\n   [<Remote host>:<port>] - Optional, if not supplied you will be prompted for the host URI.");
            MainConsole.Instance.Info(
                "iwc regions"+
                "\n   Show available regions of our IWC connections");
		}

		/// <summary>
		/// Adds the console commands.
		/// </summary>
		void AddConsoleCommands()
        {
            if (MainConsole.Instance == null)
                return;
            MainConsole.Instance.Commands.AddCommand(
                "iwc connect",
                "iwc connect [<Remote host>:<port>]",
                "Add an IWC connection to another host.",
                AddIWCConnection, false, true);

			MainConsole.Instance.Commands.AddCommand(
                "iwc disconnect",
                "iwc disconnect [<Remote host>:<port>]",
                "Disconnect IWC linking to another host.",
                RemoveIWCConnection, false, true);

            MainConsole.Instance.Commands.AddCommand(
                "iwc show", 
                "iwc show [connections]",
                "Shows all active IWC connections.",
                ShowIWCConnections, false, true);

            MainConsole.Instance.Commands.AddCommand(
                "iwc host regions",
                "iwc host regions [<Remote host>:<port>]",
                "Show available regions of an IWC host.",
                RegionsIWCHost, false, true);

            MainConsole.Instance.Commands.AddCommand(
                "iwc regions",
                "iwc regions",
                "Show available regions of our IWC connections.",
                RegionsIWCConnected, false, true);

        }

        #region Commands

        /// <summary>
        /// Checks the iwc URL entered and expands it if necessary
        /// </summary>
        /// <returns>The iwc URL</returns>
        /// <param name="iwcUrl">Iwc URL.</param>
        string CheckIwcURL(string iwcUrl)
        {
            //Be user friendly, add the http:// if needed as well as the final /
            iwcUrl = (iwcUrl.StartsWith("http://") || iwcUrl.StartsWith("https://")) ? iwcUrl : "http://" + iwcUrl;
            iwcUrl = iwcUrl.EndsWith("/") ? iwcUrl + "iwcconnection" : iwcUrl + "/iwcconnection";

            return iwcUrl;
        }

		/// <summary>
        /// Add an IWC connection.
		/// </summary>
		/// <param name="scene">Scene. (not used)</param>
		/// <param name="cmd">Cmd. optional connection URI</param>
        void AddIWCConnection(IScene scene, string[] cmd)
        {

			string iwcUrl;
            iwcUrl = cmd.Length > 2 ? cmd [2] : MainConsole.Instance.Prompt ("Url to the connection <hostname>:<port> : ");
            iwcUrl = CheckIwcURL (iwcUrl);

            if ( !OutgoingIWCComms.AttemptConnection(iwcUrl) )
            {
                MainConsole.Instance.Info (" Failed to connect to " + iwcUrl);
                return;
            }

			// we have a link, now need some details
            OSDMap response;
            OutgoingIWCComms.RequestGridUris(iwcUrl, out response);
            if ( response == null || !response ["Success"])
            {
                MainConsole.Instance.Info (" Error retrieving remote grid information");
                return;
            }

            SaveIWCUris (iwcUrl, response);

            // get the remote region details
            OutgoingIWCComms.RequestRegions (iwcUrl, out response);
            if (response == null || !response ["Success"])
            {
                MainConsole.Instance.Info (" Error receiving remote region details");
                return;
            }

            if (RegisterIWCRegions (iwcUrl, response))
            {
                // Whoo..hoo...
                MainConsole.Instance.Info (" Connected and registered IWC grid " + iwcUrl);
                return;
            }

            MainConsole.Instance.Info (" Unable to register remote grid regions");

        }

		/// <summary>
        /// Remove an IWC connection.
		/// </summary>
		/// <param name="scene">Scene. (not used)</param>
		/// <param name="cmd">Cmd. optional connection URI</param>
		void RemoveIWCConnection(IScene scene, string[] cmd)
        {

			string iwcUrl;
            if (cmd.Length > 2)
			{
				iwcUrl = cmd [2];
			} else
			{
				iwcUrl = MainConsole.Instance.Prompt ("Uri to disconnect <hostname>:<port> : ");
			}
            iwcUrl = CheckIwcURL (iwcUrl);

            if ( OutgoingIWCComms.RemoveConnection(iwcUrl) )
			{
                //Connections.Remove (iwcUrl);
				MainConsole.Instance.Info (" Disconnected from " + iwcUrl);
			} else
			{
				MainConsole.Instance.Info (" Failed to disconnect " + iwcUrl);
			}
        }

		/// <summary>
        /// Show current IWC connections.
		/// </summary>
		/// <param name="scene">Scene. (not used)</param>
		/// <param name="cmds">Cmds. (not used)</param>
		void ShowIWCConnections(IScene scene, string[] cmds)
        {

			if (Connections.Count == 0)
			{
				MainConsole.Instance.InfoFormat ("No active IWC connections.");
			} else
			{
				MainConsole.Instance.InfoFormat ("Showing {0} active IWC connections.", Connections.Count);
				foreach (string t in Connections)
				{
					MainConsole.Instance.Info ("Url: " + t);
				}
			}
        }

        /// <summary>
        /// Show an IWC enabled host's region details.
        /// </summary>
        /// <param name="scene">Scene.</param>
        /// <param name="cmd">Cmd.</param>
        void RegionsIWCHost(IScene scene, string[] cmd)
        {

            string iwcUrl;
            if (cmd.Length > 2)
            {
                iwcUrl = cmd [2];
            } else
            {
                iwcUrl = MainConsole.Instance.Prompt ("Url to the IWC host <hostname>:<port> : ");
            }

            iwcUrl = CheckIwcURL (iwcUrl);

            OSDMap response;
            if (OutgoingIWCComms.RequestRegions(iwcUrl, out response))
            {
                foreach (var r in response)
                    MainConsole.Instance.Info (" Region : " + r);
            } else
            {
                MainConsole.Instance.Info (" Failed to connect to " + iwcUrl);
            }
        }

        /// <summary>
        /// Show all regions of a previously connected IWC host.
        /// </summary>
        /// <param name="scene">Scene.</param>
        /// <param name="cmd">Cmd.</param>
        void RegionsIWCConnected(IScene scene, string[] cmd)
        {

            string iwcUrl;
            if (Connections.Count == 0)
            {
                MainConsole.Instance.Info ("There are currently no IWC connections.");
                return;
            }

            foreach ( var iwcHost in Connections)
            {
                iwcUrl = iwcHost;
                iwcUrl = CheckIwcURL (iwcUrl);

                 OSDMap response;
                if (OutgoingIWCComms.RequestRegions(iwcUrl, out response))
                {
                    foreach (var r in response)
                        MainConsole.Instance.Info (" Region : " + r);
                } else
                {
                    MainConsole.Instance.Info (" Failed to connect to " + iwcUrl);
                }
            }
        }

        #endregion
		#endregion


		#region InterWorldCommunications Members

        void ContactOtherServers()
        {
        }

        internal void AddNewConnectionFromRequest(string iwcUrl, OSDMap newUris)
        {
            if (!Connections.Contains(iwcUrl))                // add to our local connection list
                Connections.Add (iwcUrl);

        }

        internal void SaveIWCUris (string iwcUrl , OSDMap newUris)
        {
            IConfigurationService configService = Registry.RequestModuleInterface<IConfigurationService>();
            if (configService != null)
            {
                // remove any old URLs they may have sent previously
                configService.RemoveIwcURIs(iwcUrl);
                // annd in the new ones
                configService.AddIwcUrls (iwcUrl, newUris);
            }

        }

        /// <summary>
        /// Removes the requested connection details.
        /// </summary>
        /// <param name="identifer">Identifer.</param>
        internal void RemoveConnectionFromRequest(string identifer)
        {
            IConfigurationService configService = Registry.RequestModuleInterface<IConfigurationService>();
            if (configService != null)
            {
                // remove config URI's
                configService.RemoveIwcURIs(identifer);

                // and our local connection list
                Connections.Remove (identifer);

            }
        }

        internal bool RegisterIWCRegions (string iwcUrl, OSDMap regionInfo)
        {
            bool success = true;

            IGridRegistrationService gridReg = Registry.RequestModuleInterface<IGridRegistrationService>();
            if (gridReg != null)
            {
                gridReg.RemoveUrlsForClient(iwcUrl);            // probably already done

                // loop through to get region details and register them
                // ..... etc

               
            }

            return success;
        }

	}

   
	#region IWCOutgoingConnections
	/// <summary>
    ///   This class deals with sending requests to other IWC hosts
    /// </summary>
    public class IWCOutgoingConnections
    {
        readonly InterWorldCommunications IWC;

        public IWCOutgoingConnections(InterWorldCommunications iwc)
        {
            IWC = iwc;
        }

		/// <summary>
        /// Gets the OSD map from the remote server.
		///  (duplicated from ConnectorBase.cs for convienence
		/// </summary>
		/// <returns><c>true</c>, if OSD map was gotten, <c>false</c> otherwise.</returns>
		/// <param name="url">URL.</param>
		/// <param name="map">Map.</param>
		/// <param name="response">Response.</param>
        bool GetOSDMap(string url, OSDMap map, out OSDMap response)
		{
			response = null;
			string resp = WebUtils.PostToService(url, map);

			if (string.IsNullOrEmpty (resp) || resp.StartsWith ("<"))
				return false;
			try
			{
				response = (OSDMap) OSDParser.DeserializeJson(resp);
			}
			catch
			{
				response = null;
				return false;
			}
			return response["Success"];
		}

		/// <summary>
		/// Attempts to connect to the given host
		/// </summary>
		/// <returns><c>true</c>, if connection was attempted, <c>false</c> otherwise.</returns>
		/// <param name="host">The host URI to connect to</param>
        public bool AttemptConnection(string host)
        {
            IGridRegistrationService gridReg = IWC.Registry.RequestModuleInterface<IGridRegistrationService>();
            if (gridReg != null)
            {
                gridReg.RemoveUrlsForClient(host);
                IWC.IsGettingUrlsForIWCConnection = true;
                OSDMap callThem = gridReg.GetUrlForRegisteringClient(host);
                IWC.IsGettingUrlsForIWCConnection = false;
                callThem["OurIdentifier"] = Utilities.GetAddress();
				callThem["Method"] = "ConnectionRequest";
	
				OSDMap response;
                if (GetOSDMap(host, callThem, out response))
                //OSDMap result = WebUtils.PostToServiceOSDMap(host, callThem);
                    //if (result["Success"])
				{
                    if (response ["Success"])
                    {
                        // We have an initial connection... 
                        string identifier = response ["OurIdentifier"];
                        IWC.AddNewConnectionFromRequest (identifier, response);  // save the connection
                        MainConsole.Instance.Warn ("Successfully Connected to " + host);

                        return true;
                    }
                }
            }
            return false;
        }


		/// <summary>
		/// Removes the specified connection information.
		/// </summary>
		/// <returns><c>true</c>, if connection was removed, <c>false</c> otherwise.</returns>
		/// <param name="host">Host URI to remove.</param>
		public bool RemoveConnection(string host)
		{
            IGridRegistrationService gridReg = IWC.Registry.RequestModuleInterface<IGridRegistrationService>();
            if (gridReg != null)
			{
                gridReg.RemoveUrlsForClient(host);

                OSDMap callThem = gridReg.GetUrlForRegisteringClient(host);
                callThem["OurIdentifier"] = Utilities.GetAddress();
                callThem["Method"] = "DisconnectRequest";

                OSDMap response;
                if (GetOSDMap (host, callThem, out response))
                {
                    string identifier = response ["OurIdentifier"];
                    IWC.RemoveConnectionFromRequest (identifier);
                    MainConsole.Instance.Warn ("Successfully disconnected from " + host);
                    return true;
                }
			}
			return false;
		}

        /// <summary>
        /// Informs URls associated with hodt url (standalone only??).
        /// </summary>
        /// <returns><c>true</c>, if of UR ls was informed, <c>false</c> otherwise.</returns>
        /// <param name="host">Host.</param>
        /// <param name="urls">Urls.</param>
        /// <param name="userID">User Id.</param>
        /// <param name="regionHandle">Region handle.</param>
        public bool InformOfURLs(string host, OSDMap urls, UUID userID, ulong regionHandle)
        {
            urls["OurIdentifier"] = Utilities.GetAddress();
            urls["UserID"] = userID;
            urls["RegionHandle"] = regionHandle;

            urls["Method"] = "NewURLs";

			OSDMap response;
			if (GetOSDMap (host, urls, out response))
			{
				return (response ["Success"]);
			}

			return false;

        }

        public bool RequestGridUris(string host, out OSDMap response)
        {
            response = null;
            IGridRegistrationService module = IWC.Registry.RequestModuleInterface<IGridRegistrationService>();
            if (module != null)
            {
                OSDMap request = new OSDMap();
                request ["OurIdentifier"] = Utilities.GetAddress ();
                request ["Method"] = "GridInfoUris";

                if (GetOSDMap (host, request, out response))
                    return (response ["Success"]);

            }
            return false;
        }

        /// <summary>
        /// Requests the available IWC regions.
        /// </summary>
        /// <returns><c>true</c>, if regions was requested, <c>false</c> otherwise.</returns>
        /// <param name="host">Host.</param>
        /// <param name="response">Response.</param>
        public bool RequestRegions(string host, out OSDMap response)
        {
            response = null;
            IGridRegistrationService module = IWC.Registry.RequestModuleInterface<IGridRegistrationService>();
            if (module != null)
            {
                OSDMap request = new OSDMap();
                request ["OurIdentifier"] = Utilities.GetAddress ();
                request ["Method"] = "RequestRegions";

                if (GetOSDMap (host, request, out response))
                    return (response ["Success"]);

            }
            return false;
        }

        /// <summary>
        /// Requests a specified IWC region info.
        /// </summary>
        /// <returns><c>true</c>, if region info was requested, <c>false</c> otherwise.</returns>
        /// <param name="host">Host.</param>
        /// <param name="regionID">Region ID.</param>
        /// <param name="response">Response.</param>
        public bool RequestRegionInfo(string host, string regionID, out OSDMap response)
        {
            response = null;
            IGridRegistrationService gridReg = IWC.Registry.RequestModuleInterface<IGridRegistrationService>();
            if (gridReg != null)
            {
                OSDMap request = new OSDMap();
                request ["OurIdentifier"] = Utilities.GetAddress ();
                request ["Method"] = "RegionInfo";
                request ["RegionID"] = regionID;

                if (GetOSDMap (host, request, out response))
                    return (response ["Success"]);

            }
            return false;
        }

    }

	#endregion

	#region IWCIncomingConnections
    /// <summary>
    ///   This class deals with incoming requests (secure and insecure) from other hosts
    /// </summary>
	public class IWCIncomingConnections
    {
        readonly InterWorldCommunications IWC;

        public IWCIncomingConnections (InterWorldCommunications iwc)
        {
//			// IWC. AddStreamHandler(new GenericStreamHandler("POST", "/iwcconnection", HandleIncomingIWC);
			MainServer.Instance.AddStreamHandler(new GenericStreamHandler("POST", "/iwcconnection", HandleIncomingIWC));
			IWC = iwc;

	     }

        byte[] HandleIncomingIWC(string path, Stream requestData,
                                      OSHttpRequest httpRequest, OSHttpResponse httpResponse)
        {
            StreamReader sr = new StreamReader(requestData);
            string body = sr.ReadToEnd();
            sr.Close();
            body = body.Trim();

            OSDMap args = WebUtils.GetOSDMap(body,true);
            if (args == null)
            {
                //No data or not an json OSDMap
                return new byte[0];
            }
            else
            {
                if (args.ContainsKey("Method"))
                {
                    string Method = args["Method"].AsString();
                    if (Method == "ConnectionRequest")
                        return ConnectionRequest(args);
                    if (Method == "DisconnectRequest")
                        return DisconnectRequest(args);

                    // info requests
                    if (Method == "GridInfoUris")
                        return LocalGridInfo();
                    if (Method == "NewURLs")
                        return NewURLs(args);
                    if (Method == "RequestRegions")
                        return LocalRegions ();
                    if (Method == "RegionInfo")
                        return LocalRegionInfo (args);
                }
            }
            return new byte[0];
        }

        byte[] NewURLs(OSDMap args)
        {
            UUID userID = args["UserID"];
            ulong regionHandle = args["RegionHandle"];
            // was this used previously?? // string ident = userID + "|" + regionHandle;
            // ?? IWC.AddNewConnectionFromRequest(userID.ToString(), args);
            OSDMap result = new OSDMap();
            result["Success"] = true;
            string json = OSDParser.SerializeJsonString(result);
            UTF8Encoding enc = new UTF8Encoding();
            return enc.GetBytes(json);
        }

        byte[] ConnectionRequest(OSDMap args)
        {
            OSDMap result = new OSDMap();

            IGridRegistrationService gridReg = IWC.Registry.RequestModuleInterface<IGridRegistrationService>();
            if (gridReg != null)
            {
                //Add our URLs for them so that they can connect too
                string theirIdent = args["OurIdentifier"];
                UUID handle;
                if (UUID.TryParse(theirIdent, out handle))
                {
                    //Fu**in hackers
                    //No making region handle sessionIDs!
                    result["Success"] = false;
                }
                else
                {
                    gridReg.RemoveUrlsForClient(theirIdent);
                    IWC.IsGettingUrlsForIWCConnection = true;
                    result = gridReg.GetUrlForRegisteringClient(theirIdent);
                    //result = IWC.GetLocalRegionURIs (result);

                    IWC.IsGettingUrlsForIWCConnection = false;
                    MainConsole.Instance.Warn(theirIdent + " successfully connected to us");
                    IWC.AddNewConnectionFromRequest(theirIdent, args);

                    //result ["GridName"] = IWC.GridName;
                    //result ["Version"] = Utilities.WhiteCoreServerVersion();
                    result["OurIdentifier"] = Utilities.GetAddress();
                    result ["Success"] = true;
   
                }
            }

            string json = OSDParser.SerializeJsonString(result);
            UTF8Encoding enc = new UTF8Encoding();
            return enc.GetBytes(json);
        }

        byte[] DisconnectRequest(OSDMap args)
        {
            OSDMap result = new OSDMap();
            result["OurIdentifier"] = Utilities.GetAddress();
            result["Success"] = false;

            IGridRegistrationService gridReg = IWC.Registry.RequestModuleInterface<IGridRegistrationService>();
            if (gridReg != null)
            {
                string theirIdent = args["OurIdentifier"];

                // check for nasties...
                UUID handle;
                if (UUID.TryParse(theirIdent, out handle))
                {
                    //Fu**in hackers
                    //No making region handle sessionIDs!
                    result["Success"] = false;
                }
                else
                {
                    gridReg.RemoveUrlsForClient(theirIdent);
                    MainConsole.Instance.Warn(theirIdent + " disconnected from us");
                    IWC.RemoveConnectionFromRequest(theirIdent);

                    result["Success"] = true;

                }
            }

            string json = OSDParser.SerializeJsonString(result);
            UTF8Encoding enc = new UTF8Encoding();
            return enc.GetBytes(json);
        }

        byte[] LocalGridInfo()
        {

            OSDMap response =  IWC.GetLocalGridURIs();

            string json = OSDParser.SerializeJsonString(response);
            UTF8Encoding enc = new UTF8Encoding();
            return enc.GetBytes(json);
        }

        byte[] LocalRegions()
        {
            OSDMap response =  IWC.GetLocalRegions();

            string json = OSDParser.SerializeJsonString(response);
            UTF8Encoding enc = new UTF8Encoding();
            return enc.GetBytes(json);
        }

        byte[] LocalRegionInfo(OSDMap args)
        {

            string regionID = args["RegionID"];
            OSDMap response =  IWC.GetIWCRegion(regionID);

            string json = OSDParser.SerializeJsonString(response);
            UTF8Encoding enc = new UTF8Encoding();
            return enc.GetBytes(json);
        }

    }

	#endregion
	#endregion


}
