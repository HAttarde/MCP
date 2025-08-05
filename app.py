import os, re, json, ast, asyncio
import pandas as pd
import streamlit as st
import base64
from io import BytesIO
from PIL import Image
from langchain_groq import ChatGroq
from langchain_core.messages import HumanMessage, SystemMessage
from fastmcp import Client
from fastmcp.client.transports import StreamableHttpTransport
import streamlit.components.v1 as components
import re
from dotenv import load_dotenv

load_dotenv()

# Initialize Groq client with environment variable
GROQ_API_KEY = os.environ.get("GROQ_API_KEY")
if not GROQ_API_KEY:
    st.error("🔐 GROQ_API_KEY environment variable is not set. Please add it to your environment.")
    st.stop()

groq_client = ChatGroq(
    groq_api_key=GROQ_API_KEY,
    model_name=os.environ.get("GROQ_MODEL", "deepseek-r1-distill-llama-70b")
)

# ========== PAGE CONFIG ==========
st.set_page_config(page_title="MCP CRUD Chat", layout="wide")

# ========== GLOBAL CSS ==========
st.markdown("""
    <style>
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #4286f4 0%, #397dd2 100%);
        color: #fff !important;
        min-width: 330px !important;
        padding: 0 0 0 0 !important;
    }
    [data-testid="stSidebar"] .sidebar-title {
        color: #fff !important;
        font-weight: bold;
        font-size: 2.2rem;
        letter-spacing: -1px;
        text-align: center;
        margin-top: 28px;
        margin-bottom: 18px;
    }
    .sidebar-block {
        width: 94%;
        margin: 0 auto 18px auto;
    }
    .sidebar-block label {
        color: #fff !important;
        font-weight: 500;
        font-size: 1.07rem;
        margin-bottom: 4px;
        margin-left: 2px;
        display: block;
        text-align: left;
    }
    .sidebar-block .stSelectbox>div {
        background: #fff !important;
        color: #222 !important;
        border-radius: 13px !important;
        font-size: 1.13rem !important;
        min-height: 49px !important;
        box-shadow: 0 3px 14px #0002 !important;
        padding: 3px 10px !important;
        margin-top: 4px !important;
        margin-bottom: 0 !important;
    }
    .stButton>button {
            width: 100%;
            height: 3rem;
            background: #39e639;
            color: #222;
            font-size: 1.1rem;
            font-weight: bold;
            border-radius: 10px;
            margin-bottom: 2rem;
        }
    /* Small refresh button styling */
    .small-refresh-button button {
        width: auto !important;
        height: 2rem !important;
        background: #4286f4 !important;
        color: #fff !important;
        font-size: 0.85rem !important;
        font-weight: 500 !important;
        border-radius: 6px !important;
        margin-bottom: 0.5rem !important;
        padding: 0.25rem 0.75rem !important;
        border: none !important;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1) !important;
    }
    .small-refresh-button button:hover {
        background: #397dd2 !important;
        transform: translateY(-1px) !important;
        box-shadow: 0 4px 8px rgba(0,0,0,0.15) !important;
    }
    .sidebar-logo-label {
        margin-top: 30px !important;
        margin-bottom: 10px;
        font-size: 1.13rem !important;
        font-weight: 600;
        text-align: center;
        color: #fff !important;
        letter-spacing: 0.1px;
    }
    .sidebar-logo-row {
        display: flex;
        flex-direction: row;
        justify-content: center;
        align-items: center;
        gap: 20px;
        margin-top: 8px;
        margin-bottom: 8px;
    }
    .sidebar-logo-row img {
        width: 42px;
        height: 42px;
        border-radius: 9px;
        background: #fff;
        padding: 6px 8px;
        object-fit: contain;
        box-shadow: 0 2px 8px #0002;
    }
    /* Chat area needs bottom padding so sticky bar does not overlap */
    .stChatPaddingBottom { padding-bottom: 98px; }
    /* Responsive sticky chatbar */
    .sticky-chatbar {
        position: fixed;
        left: 330px;
        right: 0;
        bottom: 0;
        z-index: 100;
        background: #f8fafc;
        padding: 0.6rem 2rem 0.8rem 2rem;
        box-shadow: 0 -2px 24px #0001;
    }
    @media (max-width: 800px) {
        .sticky-chatbar { left: 0; right: 0; padding: 0.6rem 0.5rem 0.8rem 0.5rem; }
        [data-testid="stSidebar"] { display: none !important; }
    }
    .chat-bubble {
        padding: 13px 20px;
        margin: 8px 0;
        border-radius: 18px;
        max-width: 75%;
        font-size: 1.09rem;
        line-height: 1.45;
        box-shadow: 0 1px 4px #0001;
        display: inline-block;
        word-break: break-word;
    }
    .user-msg {
        background: #e6f0ff;
        color: #222;
        margin-left: 24%;
        text-align: right;
        border-bottom-right-radius: 6px;
        border-top-right-radius: 24px;
    }
    .agent-msg {
        background: #f5f5f5;
        color: #222;
        margin-right: 24%;
        text-align: left;
        border-bottom-left-radius: 6px;
        border-top-left-radius: 24px;
    }
    .chat-row {
        display: flex;
        align-items: flex-end;
        margin-bottom: 0.6rem;
    }
    .avatar {
        height: 36px;
        width: 36px;
        border-radius: 50%;
        margin: 0 8px;
        object-fit: cover;
        box-shadow: 0 1px 4px #0002;
    }
    .user-avatar { order: 2; }
    .agent-avatar { order: 0; }
    .user-bubble { order: 1; }
    .agent-bubble { order: 1; }
    .right { justify-content: flex-end; }
    .left { justify-content: flex-start; }
    .chatbar-claude {
        display: flex;
        align-items: center;
        gap: 12px;
        width: 100%;
        max-width: 850px;
        margin: 0 auto;
        border-radius: 20px;
        background: #fff;
        box-shadow: 0 2px 8px #0002;
        padding: 8px 14px;
        margin-bottom: 0;
    }
    .claude-hamburger {
        background: #f2f4f9;
        border: none;
        border-radius: 11px;
        font-size: 1.35rem;
        font-weight: bold;
        width: 38px; height: 38px;
        cursor: pointer;
        display: flex; align-items: center; justify-content: center;
        transition: background 0.13s;
    }
    .claude-hamburger:hover { background: #e6f0ff; }
    .claude-input {
        flex: 1;
        border: none;
        outline: none;
        font-size: 1.12rem;
        padding: 0.45rem 0.5rem;
        background: #f5f7fa;
        border-radius: 8px;
        min-width: 60px;
    }
    .claude-send {
        background: #fe3044 !important;
        color: #fff !important;
        border: none;
        border-radius: 50%;
        width: 40px; height: 40px;
        font-size: 1.4rem !important;
        cursor: pointer;
        display: flex; align-items: center; justify-content: center;
        transition: background 0.17s;
    }
    .claude-send:hover { background: #d91d32 !important; }
    .tool-menu {
        position: fixed;
        top: 20px;
        right: 20px;
        background: #fff;
        border: 1px solid #ddd;
        border-radius: 8px;
        padding: 16px;
        box-shadow: 0 4px 12px rgba(0,0,0,0.15);
        z-index: 1000;
        min-width: 200px;
    }
    .server-title {
        font-weight: bold;
        margin-bottom: 10px;
        color: #333;
    }
    .expandable {
        margin-top: 8px;
    }
    [data-testid="stSidebar"] .stSelectbox label {
        color: #fff !important;
        font-weight: 500;
        font-size: 1.07rem;
    }
    </style>
""", unsafe_allow_html=True)


# ========== DYNAMIC TOOL DISCOVERY FUNCTIONS ==========
async def _discover_tools() -> dict:
    """Discover available tools from the MCP server"""
    try:
        transport = StreamableHttpTransport(f"{st.session_state.get('MCP_SERVER_URL', 'http://localhost:8000')}/mcp")
        async with Client(transport) as client:
            tools = await client.list_tools()
            return {tool.name: tool.description for tool in tools}
    except Exception as e:
        st.error(f"Failed to discover tools: {e}")
        return {}


def discover_tools() -> dict:
    """Synchronous wrapper for tool discovery"""
    return asyncio.run(_discover_tools())


def generate_tool_descriptions(tools_dict: dict) -> str:
    """Generate tool descriptions string from discovered tools"""
    if not tools_dict:
        return "No tools available"

    descriptions = ["Available tools:"]
    for i, (tool_name, tool_desc) in enumerate(tools_dict.items(), 1):
        descriptions.append(f"{i}. {tool_name}: {tool_desc}")

    return "\n".join(descriptions)

def get_image_base64(img_path):
    img = Image.open(img_path)
    buffered = BytesIO()
    img.save(buffered, format="PNG")
    img_bytes = buffered.getvalue()
    img_base64 = base64.b64encode(img_bytes).decode()
    return img_base64

# ========== SIDEBAR NAVIGATION ==========
with st.sidebar:
    st.markdown("<div class='sidebar-title'>Solutions Scope</div>", unsafe_allow_html=True)
    with st.container():
        # Application selectbox (with key)
        application = st.selectbox(
            "Select Application",
            ["Select Application", "MCP Application"],
            key="app_select"
        )

        # Dynamically choose default options for other selects
        # Option lists
        protocol_options = ["", "MCP Protocol", "A2A Protocol"]
        llm_options = ["", "Groq Llama3-70B", "Groq Llama3-8B", "Groq Mixtral-8x7B", "Groq Gemma"]

        # Logic to auto-select defaults if MCP Application is chosen
        protocol_index = protocol_options.index(
            "MCP Protocol") if application == "MCP Application" else protocol_options.index(
            st.session_state.get("protocol_select", ""))
        llm_index = llm_options.index("Groq Llama3-70B") if application == "MCP Application" else llm_options.index(
            st.session_state.get("llm_select", ""))

        protocol = st.selectbox(
            "Protocol",
            protocol_options,
            key="protocol_select",
            index=protocol_index
        )

        llm_model = st.selectbox(
            "LLM Models",
            llm_options,
            key="llm_select",
            index=llm_index
        )

        # Dynamic server tools selection based on discovered tools
        if application == "MCP Application" and "available_tools" in st.session_state and st.session_state.available_tools:
            server_tools_options = [""] + list(st.session_state.available_tools.keys())
            default_tool = list(st.session_state.available_tools.keys())[0] if st.session_state.available_tools else ""
            server_tools_index = server_tools_options.index(default_tool) if default_tool else 0
        else:
            server_tools_options = ["", "sqlserver_crud", "postgresql_crud"]  # Fallback
            server_tools_index = 0

        server_tools = st.selectbox(
            "Server Tools",
            server_tools_options,
            key="server_tools",
            index=server_tools_index
        )

        st.button("Clear/Reset", key="clear_button")

    st.markdown('<div class="sidebar-logo-label">Build & Deployed on</div>', unsafe_allow_html=True)
    logo_base64 = get_image_base64("Picture1.png")
    st.markdown(
    f"""
    <div class="sidebar-logo-row">
        <img src="https://media.licdn.com/dms/image/v2/D560BAQFIon13R1UG4g/company-logo_200_200/company-logo_200_200/0/1733990910443/llm_at_scale_logo?e=2147483647&v=beta&t=WtAgFOcGQuTS0aEIqZhNMzWraHwL6FU0z5EPyPrty04" title="Logo" style="width: 50px; height: 50px;">
        <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/googlecloud/googlecloud-original.svg" title="Google Cloud" style="width: 50px; height: 50px;">
        <img src="https://a0.awsstatic.com/libra-css/images/logos/aws_logo_smile_1200x630.png" title="AWS" style="width: 50px; height: 50px;">
        <img src="https://upload.wikimedia.org/wikipedia/commons/a/a8/Microsoft_Azure_Logo.svg" title="Azure Cloud" style="width: 50px; height: 50px;">
    </div>
    """,
    unsafe_allow_html=True
)


# ========== LOGO/HEADER FOR MAIN AREA ==========



logo_path = "Picture1.png"
logo_base64 = get_image_base64(logo_path) if os.path.exists(logo_path) else ""
if logo_base64:
    st.markdown(
        f"""
        <div style='display: flex; flex-direction: column; align-items: center; margin-bottom:20px;'>
            <img src='data:image/png;base64,{logo_base64}' width='220'>
        </div>
        """,
        unsafe_allow_html=True
    )

st.markdown(
    """
    <div style="
        display: flex;
        flex-direction: column;
        align-items: center;
        margin-bottom: 18px;
        padding: 10px 0 10px 0;
    ">
        <span style="
            font-size: 2.5rem;
            font-weight: bold;
            letter-spacing: -2px;
            color: #222;
        ">
            Data Exchange with Natural Language 
        </span>
        <span style="
            font-size: 1.15rem;
            color: #555;
            margin-top: 0.35rem;
        ">
            Agentic Approach:  NO SQL, NO ETL, NO DATA WAREHOUSING, NO BI TOOL 
        </span>
        <hr style="
        width: 80%;
        border: none;
        height: 2px;
        background: linear-gradient(90deg, transparent, #4286f4, transparent);
        margin: 20px auto;
        ">
    </div>

    """,
    unsafe_allow_html=True
)

# ========== SESSION STATE INIT ==========
if "messages" not in st.session_state:
    st.session_state.messages = []

# Initialize available_tools if not exists
if "available_tools" not in st.session_state:
    st.session_state.available_tools = {}

# Initialize MCP_SERVER_URL in session state
if "MCP_SERVER_URL" not in st.session_state:
    st.session_state["MCP_SERVER_URL"] = os.getenv("MCP_SERVER_URL", "http://localhost:8000")

# Initialize tool_states dynamically based on discovered tools
if "tool_states" not in st.session_state:
    st.session_state.tool_states = {}

if "show_menu" not in st.session_state:
    st.session_state["show_menu"] = False
if "menu_expanded" not in st.session_state:
    st.session_state["menu_expanded"] = True
if "chat_input_box" not in st.session_state:
    st.session_state["chat_input_box"] = ""


# ========== HELPER FUNCTIONS ==========
def _clean_json(raw: str) -> str:
    fences = re.findall(r"``````", raw, re.DOTALL)
    if fences:
        return fences[0].strip()
    # If no JSON code fence, try to find JSON-like content
    json_match = re.search(r'\{.*\}', raw, re.DOTALL)
    return json_match.group(0).strip() if json_match else raw.strip()


# ========== PARAMETER VALIDATION FUNCTION ==========
def validate_and_clean_parameters(tool_name: str, args: dict) -> dict:
    """Validate and clean parameters for specific tools"""

    if tool_name == "sales_crud":
        # Define allowed parameters for sales_crud (with WHERE clause support)
        allowed_params = {
            'operation', 'customer_id', 'product_id', 'quantity',
            'unit_price', 'total_amount', 'sale_id', 'new_quantity',
            'table_name', 'display_format', 'customer_name',
            'product_name', 'email', 'total_price',
            'columns',  # Column selection
            'where_clause',  # WHERE conditions
            'filter_conditions',  # Structured filters
            'limit'  # Row limit
        }

        # Clean args to only include allowed parameters
        cleaned_args = {k: v for k, v in args.items() if k in allowed_params}

        # Validate display_format values
        if 'display_format' in cleaned_args:
            valid_formats = [
                'Data Format Conversion',
                'Decimal Value Formatting',
                'String Concatenation',
                'Null Value Removal/Handling'
            ]
            if cleaned_args['display_format'] not in valid_formats:
                cleaned_args.pop('display_format', None)

        # Clean up columns parameter
        if 'columns' in cleaned_args:
            if isinstance(cleaned_args['columns'], str) and cleaned_args['columns'].strip():
                columns_str = cleaned_args['columns'].strip()
                columns_list = [col.strip() for col in columns_str.split(',') if col.strip()]
                cleaned_args['columns'] = ','.join(columns_list)
            else:
                cleaned_args.pop('columns', None)

        # Validate WHERE clause
        if 'where_clause' in cleaned_args:
            if not isinstance(cleaned_args['where_clause'], str) or not cleaned_args['where_clause'].strip():
                cleaned_args.pop('where_clause', None)

        # Validate limit
        if 'limit' in cleaned_args:
            try:
                limit_val = int(cleaned_args['limit'])
                if limit_val <= 0 or limit_val > 1000:  # Reasonable limits
                    cleaned_args.pop('limit', None)
                else:
                    cleaned_args['limit'] = limit_val
            except (ValueError, TypeError):
                cleaned_args.pop('limit', None)

        return cleaned_args

    elif tool_name == "sqlserver_crud":
        allowed_params = {
            'operation', 'name', 'email', 'limit', 'customer_id',
            'new_email', 'table_name'
        }
        return {k: v for k, v in args.items() if k in allowed_params}

    elif tool_name == "postgresql_crud":
        allowed_params = {
            'operation', 'name', 'price', 'description', 'limit',
            'product_id', 'new_price', 'table_name'
        }
        return {k: v for k, v in args.items() if k in allowed_params}

    return args


# ========== NEW LLM RESPONSE GENERATOR ==========
def generate_llm_response(operation_result: dict, action: str, tool: str, user_query: str) -> str:
    """Generate LLM response based on operation result with context"""

    # Prepare context for LLM
    context = {
        "action": action,
        "tool": tool,
        "user_query": user_query,
        "operation_result": operation_result
    }

    system_prompt = (
        "You are a helpful database assistant. Generate a brief, natural response "
        "explaining what operation was performed and its result. Be conversational "
        "and informative. Focus on the business context and user-friendly explanation."
    )

    user_prompt = f"""
    Based on this database operation context, generate a brief natural response:

    User asked: "{user_query}"
    Operation: {action}
    Tool used: {tool}
    Result: {json.dumps(operation_result, indent=2)}

    Generate a single line response explaining what was done and the outcome.
    """

    try:
        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_prompt)
        ]
        response = groq_client.invoke(messages)
        return response.content.strip()
    except Exception as e:
        # Fallback response if LLM call fails
        if action == "read":
            return f"Successfully retrieved data from {tool}."
        elif action == "create":
            return f"Successfully created new record in {tool}."
        elif action == "update":
            return f"Successfully updated record in {tool}."
        elif action == "delete":
            return f"Successfully deleted record from {tool}."
        elif action == "describe":
            return f"Successfully retrieved table schema from {tool}."
        else:
            return f"Operation completed successfully using {tool}."


def parse_user_query(query: str, available_tools: dict) -> dict:
    """Enhanced parse user query with better DELETE operation handling"""

    if not available_tools:
        return {"error": "No tools available"}

    # Build comprehensive tool information for the LLM
    tool_info = []
    for tool_name, tool_desc in available_tools.items():
        tool_info.append(f"- **{tool_name}**: {tool_desc}")

    tools_description = "\n".join(tool_info)

    system_prompt = (
    "You are an intelligent database router for CRUD operations. "
    "Your job is to analyze the user's query and select the most appropriate tool based on the context and data being requested.\n\n"

    "RESPONSE FORMAT:\n"
    "Reply with exactly one JSON object: {\"tool\": string, \"action\": string, \"args\": object}\n\n"

    "ACTION MAPPING:\n"
    "- 'read': for viewing, listing, showing, displaying, or getting records\n"
    "- 'create': for adding, inserting, or creating NEW records\n"
    "- 'update': for modifying, changing, or updating existing records\n"
    "- 'delete': for removing, deleting, or destroying records\n"
    "- 'describe': for showing table structure, schema, or column information\n\n"

    "CRITICAL TOOL SELECTION RULES:\n"
    "\n"
    "1. **PRODUCT QUERIES** → Use 'postgresql_crud':\n"
    "   - 'list products', 'show products', 'display products'\n"
    "   - 'product inventory', 'product catalog', 'product information'\n"
    "   - 'add product', 'create product', 'new product'\n"
    "   - 'update product', 'change product price', 'modify product'\n"
    "   - 'delete product', 'remove product', 'delete [ProductName]'\n"
    "   - Any query primarily about products, pricing, or inventory\n"
    "\n"
    "2. **CUSTOMER QUERIES** → Use 'sqlserver_crud':\n"
    "   - 'list customers', 'show customers', 'display customers'\n"
    "   - 'customer information', 'customer details'\n"
    "   - 'add customer', 'create customer', 'new customer'\n"
    "   - 'update customer', 'change customer email', 'modify customer'\n"
    "   - 'delete customer', 'remove customer', 'delete [CustomerName]'\n"
    "   - Any query primarily about customers, names, or emails\n"
    "\n"
    "3. **SALES/TRANSACTION QUERIES** → Use 'sales_crud':\n"
    "   - 'list sales', 'show sales', 'sales data', 'transactions'\n"
    "   - 'sales report', 'revenue data', 'purchase history'\n"
    "   - 'who bought what', 'customer purchases'\n"
    "   - Cross-database queries combining customer + product + sales info\n"
    "   - 'create sale', 'add sale', 'new transaction'\n"
    "   - Any query asking for combined data from multiple tables\n"
    "\n"
    "4. **CARE PLAN QUERIES** → Use 'careplan_crud':\n"
"   - 'show care plans', 'list case notes', 'display care plans', 'care plan records'\n"
"   - 'list care plans with name John', 'care plans mentioning cancer'\n"
"   - 'show care plan without address', 'display only name and notes'\n"
"   - Any query related to healthcare records with Name, Address, Phone Number, Case Notes\n\n"


    "**ETL & DISPLAY FORMATTING RULES:**\n"
    "For any data formatting requests (e.g., rounding decimals, changing date formats, handling nulls), "
    "you MUST use the `display_format` parameter within the `sales_crud` tool.\n\n"

    "1. **DECIMAL FORMATTING:**\n"
    "   - If the user asks to 'round', 'format to N decimal places', or mentions 'decimals'.\n"
    "   - Use: {\"display_format\": \"Decimal Value Formatting\"}\n"
    "   - **Example Query:** 'show sales with 2 decimal places'\n"
    "   - **→ Correct Tool Call:** {\"tool\": \"sales_crud\", \"action\": \"read\", \"args\": {\"display_format\": \"Decimal Value Formatting\"}}\n"

    "2. **DATE FORMATTING:**\n"
    "   - If the user asks to 'format date', 'show date as YYYY-MM-DD', or similar.\n"
    "   - Use: {\"display_format\": \"Data Format Conversion\"}\n"
    "   - **Example Query:** 'show sales with formatted dates'\n"
    "   - **→ Correct Tool Call:** {\"tool\": \"sales_crud\", \"action\": \"read\", \"args\": {\"display_format\": \"Data Format Conversion\"}}\n"

    "3. **NULL VALUE HANDLING:**\n"
    "   - If the user asks to 'remove nulls', 'replace empty values', or 'handle missing data'.\n"
    "   - Use: {\"display_format\": \"Null Value Removal/Handling\"}\n"
    "   - **Example Query:** 'show sales but remove records with missing info'\n"
    "   - **→ Correct Tool Call:** {\"tool\": \"sales_crud\", \"action\": \"read\", \"args\": {\"display_format\": \"Null Value Removal/Handling\"}}\n"

    "4. **STRING CONCATENATION:**\n"
    "   - If the user asks to 'combine names', 'create a full description', or 'show full name'.\n"
    "   - Use: {\"display_format\": \"String Concatenation\"}\n"
    "   - **Example Query:** 'show sales with customer full names'\n"
    "   - **→ Correct Tool Call:** {\"tool\": \"sales_crud\", \"action\": \"read\", \"args\": {\"display_format\": \"String Concatenation\"}}\n"

    "5. **CARE PLAN COLUMN FILTERING:**\n"
    "   - If the user asks to 'show only name and notes', 'remove address', or 'exclude phone number'.\n"
    "   - For EXCLUSION queries (exclude, remove, without, but not):\n"
    "   - **Example Query:** 'show care plans without phone number and address'\n"
    "   - **→ Correct Tool Call:** {\"tool\": \"careplan_crud\", \"action\": \"read\", \"args\": {\"columns\": \"exclude phone_number,address\"}}\n"
    "   - **Example Query:** 'list care plans but exclude phone number and address'\n"
    "   - **→ Correct Tool Call:** {\"tool\": \"careplan_crud\", \"action\": \"read\", \"args\": {\"columns\": \"exclude phone_number,address\"}}\n"
    "   - **Example Query:** 'show care plans without address'\n"
    "   - **→ Correct Tool Call:** {\"tool\": \"careplan_crud\", \"action\": \"read\", \"args\": {\"columns\": \"exclude address\"}}\n"
    "   - For POSITIVE selection:\n"
    "   - **Example Query:** 'show only name and case notes from care plans'\n"
    "   - **→ Correct Tool Call:** {\"tool\": \"careplan_crud\", \"action\": \"read\", \"args\": {\"columns\": \"name,case_notes\"}}\n"

    "6. **CARE PLAN FILTERING BY TEXT OR VALUE:**\n"
"   - For name-based searches, use the 'name' parameter directly\n"
"   - **Example Query:** 'show records in care plan where name is John'\n"
"   - **→ Correct Tool Call:** {\"tool\": \"careplan_crud\", \"action\": \"read\", \"args\": {\"name\": \"John\"}}\n"
"   - **Example Query:** 'care plans for Emily'\n"
"   - **→ Correct Tool Call:** {\"tool\": \"careplan_crud\", \"action\": \"read\", \"args\": {\"name\": \"Emily\"}}\n"
"   - For case notes searches, use where_clause with LIKE\n"
"   - **Example Query:** 'care plans mentioning cancer'\n"
"   - **→ Correct Tool Call:** {\"tool\": \"careplan_crud\", \"action\": \"read\", \"args\": {\"where_clause\": \"case_notes LIKE '%cancer%'\"}}\n"
"   - **Example Query:** 'show care plans for address New York'\n"
"   - **→ Correct Tool Call:** {\"tool\": \"careplan_crud\", \"action\": \"read\", \"args\": {\"where_clause\": \"address LIKE '%New York%'\"}}\n"

    "7. **CARE PLAN COLUMN NAME MAPPING:**\n"
    "   - 'name' → 'Name'\n"
    "   - 'address' → 'Address'\n"
    "   - 'phone number', 'phone' → 'PhoneNumber'\n"
    "   - 'case notes', 'notes' → 'CaseNotes'\n"
)

    user_prompt = f"""User query: "{query}"

Analyze the query step by step:

1. What is the PRIMARY INTENT? (product, customer, or sales operation)
2. What ACTION is being requested? (create, read, update, delete, describe)
3. What ENTITY NAME needs to be extracted? (for delete/update operations)
4. What SPECIFIC COLUMNS are requested? (for read operations - extract into 'columns' parameter)
5. What FILTER CONDITIONS are specified? (for read operations - extract into 'where_clause' parameter)
6. What PARAMETERS need to be extracted from the natural language?

ENTITY NAME EXTRACTION GUIDELINES (CRITICAL FOR DELETE/UPDATE):
- For "delete Widget" → extract "Widget" and put in 'name' parameter
- For "delete product Gadget" → extract "Gadget" and put in 'name' parameter  
- For "delete customer Alice" → extract "Alice" and put in 'name' parameter
- For "update price of Tool to 30" → extract "Tool" and put in 'name' parameter, extract "30" and put in 'new_price'

COLUMN EXTRACTION GUIDELINES:
- Look for patterns like "show X, Y", "display X and Y", "get X, Y from Z"
- Extract only the column names, map them to standard names
- Put them in a comma-separated string in the 'columns' parameter

WHERE CLAUSE EXTRACTION GUIDELINES:
- Look for filtering conditions like "exceed", "above", "greater than", "with price over"
- Convert natural language to SQL-like conditions
- Handle currency symbols and numbers properly
- Put the condition in the 'where_clause' parameter

Respond with the exact JSON format with properly extracted parameters."""

    try:
        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_prompt)
        ]
        resp = groq_client.invoke(messages)

        raw = _clean_json(resp.content)

        try:
            result = json.loads(raw)
        except json.JSONDecodeError:
            try:
                result = ast.literal_eval(raw)
            except:
                result = {"tool": list(available_tools.keys())[0], "action": "read", "args": {}}

        # Normalize action names
        if "action" in result and result["action"] in ["list", "show", "display", "view", "get"]:
            result["action"] = "read"

        # ENHANCED parameter extraction for DELETE and UPDATE operations
        if result.get("action") in ["delete", "update"]:
            args = result.get("args", {})
            
            # Extract entity name for delete/update operations if not already extracted
            if "name" not in args:
                import re
                
                # Enhanced regex patterns for delete operations
                delete_patterns = [
                    r'(?:delete|remove)\s+(?:product\s+)?([A-Za-z][A-Za-z0-9\s]*?)(?:\s|$)',
                    r'(?:delete|remove)\s+(?:customer\s+)?([A-Za-z][A-Za-z0-9\s]*?)(?:\s|$)',
                    r'(?:delete|remove)\s+([A-Za-z][A-Za-z0-9\s]*?)(?:\s|$)'
                ]
                
                # Enhanced regex patterns for update operations
                update_patterns = [
                    r'(?:update|change|set)\s+(?:price\s+of\s+)?([A-Za-z][A-Za-z0-9\s]*?)\s+(?:to|=|\s+)',
                    r'(?:update|change|set)\s+(?:email\s+of\s+)?([A-Za-z][A-Za-z0-9\s]*?)\s+(?:to|=|\s+)',
                    r'(?:update|change|set)\s+([A-Za-z][A-Za-z0-9\s]*?)\s+(?:price|email)\s+(?:to|=)',
                ]
                
                all_patterns = delete_patterns + update_patterns
                
                for pattern in all_patterns:
                    match = re.search(pattern, query, re.IGNORECASE)
                    if match:
                        extracted_name = match.group(1).strip()
                        # Clean up common words that might be captured
                        stop_words = ['product', 'customer', 'price', 'email', 'to', 'of', 'the', 'a', 'an']
                        name_words = [word for word in extracted_name.split() if word.lower() not in stop_words]
                        if name_words:
                            args["name"] = ' '.join(name_words)
                            print(f"DEBUG: Extracted name '{args['name']}' from query '{query}'")
                            break
            
            # Extract new_price for product updates
            if result.get("action") == "update" and result.get("tool") == "postgresql_crud" and "new_price" not in args:
                import re
                price_match = re.search(r'(?:to|=|\s+)\$?(\d+(?:\.\d+)?)', query, re.IGNORECASE)
                if price_match:
                    args["new_price"] = float(price_match.group(1))
                    print(f"DEBUG: Extracted new_price '{args['new_price']}' from query '{query}'")
            
            # Extract new_email for customer updates
            if result.get("action") == "update" and result.get("tool") == "sqlserver_crud" and "new_email" not in args:
                import re
                email_match = re.search(r'(?:to|=|\s+)([\w\.-]+@[\w\.-]+\.\w+)', query, re.IGNORECASE)
                if email_match:
                    args["new_email"] = email_match.group(1)
                    print(f"DEBUG: Extracted new_email '{args['new_email']}' from query '{query}'")
            
            result["args"] = args

        # Enhanced parameter extraction for create operations
        elif result.get("action") == "create":
            args = result.get("args", {})
            
            # Extract name and email from query if not already extracted
            if result.get("tool") == "sqlserver_crud" and ("name" not in args or "email" not in args):
                # Try to extract name and email using regex patterns
                import re
                
                # Extract email
                email_match = re.search(r'[\w\.-]+@[\w\.-]+\.\w+', query)
                if email_match and "email" not in args:
                    args["email"] = email_match.group(0)
                
                # Extract name (everything between 'customer' and 'with' or before email)
                if "name" not in args:
                    # Pattern 1: "create customer [Name] with [email]"
                    name_match = re.search(r'(?:create|add|new)\s+customer\s+([^@]+?)(?:\s+with|\s+[\w\.-]+@)', query, re.IGNORECASE)
                    if not name_match:
                        # Pattern 2: "create [Name] [email]" or "add [Name] with [email]"
                        name_match = re.search(r'(?:create|add|new)\s+([^@]+?)(?:\s+with|\s+[\w\.-]+@)', query, re.IGNORECASE)
                    if not name_match:
                        # Pattern 3: Extract everything before the email
                        if email_match:
                            name_part = query[:email_match.start()].strip()
                            name_match = re.search(r'(?:customer|create|add|new)\s+(.+)', name_part, re.IGNORECASE)
                    
                    if name_match:
                        extracted_name = name_match.group(1).strip()
                        # Clean up common words
                        extracted_name = re.sub(r'\b(with|email|named|called)\b', '', extracted_name, flags=re.IGNORECASE).strip()
                        if extracted_name:
                            args["name"] = extracted_name
            
            result["args"] = args

        # Enhanced parameter extraction for read operations with columns and where_clause
        elif result.get("action") == "read" and result.get("tool") == "sales_crud":
            args = result.get("args", {})
            
            # Extract columns if not already extracted
            if "columns" not in args:
                import re
                
                # Look for column specification patterns
                column_patterns = [
                    r'(?:show|display|get|select)\s+([^,\s]+(?:,\s*[^,\s]+)*?)(?:\s+from|\s+where|\s*$)',
                    r'(?:show|display|get|select)\s+(.+?)\s+(?:from|where)',
                    r'display\s+(.+?)(?:\s+from|\s*$)',
                ]
                
                for pattern in column_patterns:
                    match = re.search(pattern, query, re.IGNORECASE)
                    if match:
                        columns_text = match.group(1).strip()
                        
                        # Clean up and standardize column names
                        if 'and' in columns_text or ',' in columns_text:
                            # Multiple columns
                            columns_list = re.split(r'[,\s]+and\s+|,\s*', columns_text)
                            cleaned_columns = []
                            
                            for col in columns_list:
                                col = col.strip().lower().replace(' ', '_')
                                # Map common variations
                                if col in ['name', 'customer']:
                                    cleaned_columns.append('customer_name')
                                elif col in ['price', 'total', 'amount']:
                                    cleaned_columns.append('total_price')
                                elif col in ['product']:
                                    cleaned_columns.append('product_name')
                                elif col in ['date']:
                                    cleaned_columns.append('sale_date')
                                elif col in ['email']:
                                    cleaned_columns.append('customer_email')
                                else:
                                    cleaned_columns.append(col)
                            
                            if cleaned_columns:
                                args["columns"] = ','.join(cleaned_columns)
                        else:
                            # Single column
                            col = columns_text.strip().lower().replace(' ', '_')
                            if col in ['name', 'customer']:
                                args["columns"] = 'customer_name'
                            elif col in ['price', 'total', 'amount']:
                                args["columns"] = 'total_price'
                            elif col in ['product']:
                                args["columns"] = 'product_name'
                            elif col in ['date']:
                                args["columns"] = 'sale_date'
                            elif col in ['email']:
                                args["columns"] = 'customer_email'
                            else:
                                args["columns"] = col
                        break
            
            # Extract where_clause if not already extracted
            if "where_clause" not in args:
                import re
                
                # Look for filtering conditions
                where_patterns = [
                    r'(?:with|where)\s+total[_\s]*price[_\s]*(?:exceed[s]?|above|greater\s+than|more\s+than|>)\s*\$?(\d+(?:\.\d+)?)',
                    r'(?:with|where)\s+total[_\s]*price[_\s]*(?:below|less\s+than|under|<)\s*\$?(\d+(?:\.\d+)?)',
                    r'(?:with|where)\s+total[_\s]*price[_\s]*(?:equal[s]?|is|=)\s*\$?(\d+(?:\.\d+)?)',
                    r'(?:with|where)\s+quantity[_\s]*(?:>|above|greater\s+than|more\s+than)\s*(\d+)',
                    r'(?:with|where)\s+quantity[_\s]*(?:<|below|less\s+than|under)\s*(\d+)',
                    r'(?:with|where)\s+quantity[_\s]*(?:=|equal[s]?|is)\s*(\d+)',
                    r'(?:by|for)\s+customer[_\s]*([A-Za-z\s]+?)(?:\s|$)',
                    r'(?:for|of)\s+product[_\s]*([A-Za-z\s]+?)(?:\s|$)',
                ]
                
                for i, pattern in enumerate(where_patterns):
                    match = re.search(pattern, query, re.IGNORECASE)
                    if match:
                        value = match.group(1).strip()
                        
                        if i <= 2:  # total_price conditions
                            if 'exceed' in query.lower() or 'above' in query.lower() or 'greater' in query.lower() or 'more' in query.lower():
                                args["where_clause"] = f"total_price > {value}"
                            elif 'below' in query.lower() or 'less' in query.lower() or 'under' in query.lower():
                                args["where_clause"] = f"total_price < {value}"
                            else:
                                args["where_clause"] = f"total_price = {value}"
                        elif i <= 5:  # quantity conditions
                            if 'above' in query.lower() or 'greater' in query.lower() or 'more' in query.lower():
                                args["where_clause"] = f"quantity > {value}"
                            elif 'below' in query.lower() or 'less' in query.lower() or 'under' in query.lower():
                                args["where_clause"] = f"quantity < {value}"
                            else:
                                args["where_clause"] = f"quantity = {value}"
                        elif i == 6:  # customer name
                            args["where_clause"] = f"customer_name = '{value}'"
                        elif i == 7:  # product name
                            args["where_clause"] = f"product_name = '{value}'"
                        break
            
            result["args"] = args

        # Validate and clean args
        if "args" in result and isinstance(result["args"], dict):
            cleaned_args = validate_and_clean_parameters(result.get("tool"), result["args"])
            result["args"] = cleaned_args

        # Validate tool selection
        if "tool" in result and result["tool"] not in available_tools:
            result["tool"] = list(available_tools.keys())[0]

        # Debug output
        print(f"DEBUG: Final parsed result for '{query}': {result}")

        return result

    except Exception as e:
        return {
            "tool": list(available_tools.keys())[0] if available_tools else None,
            "action": "read",
            "args": {},
            "error": f"Failed to parse query: {str(e)}"
        }


async def _invoke_tool(tool: str, action: str, args: dict) -> any:
    transport = StreamableHttpTransport(f"{st.session_state['MCP_SERVER_URL']}/mcp")
    async with Client(transport) as client:
        payload = {"operation": action, **{k: v for k, v in args.items() if k != "operation"}}
        res_obj = await client.call_tool(tool, payload)
    if res_obj.structured_content is not None:
        return res_obj.structured_content
    text = "".join(b.text for b in res_obj.content).strip()
    if text.startswith("{") and "}{" in text:
        text = "[" + text.replace("}{", "},{") + "]"
    try:
        return json.loads(text)
    except:
        return text


def call_mcp_tool(tool: str, action: str, args: dict) -> any:
    return asyncio.run(_invoke_tool(tool, action, args))


def format_natural(data) -> str:
    if isinstance(data, list):
        lines = []
        for i, item in enumerate(data, 1):
            if isinstance(item, dict):
                parts = [f"{k} {v}" for k, v in item.items()]
                lines.append(f"Record {i}: " + ", ".join(parts) + ".")
            else:
                lines.append(f"{i}. {item}")
        return "\n".join(lines)
    if isinstance(data, dict):
        parts = [f"{k} {v}" for k, v in data.items()]
        return ", ".join(parts) + "."
    return str(data)


def normalize_args(args):
    mapping = {
        "product_name": "name",
        "customer_name": "name",
        "item": "name"
    }
    for old_key, new_key in mapping.items():
        if old_key in args:
            args[new_key] = args.pop(old_key)
    return args


def extract_name_from_query(text: str) -> str:
    """Enhanced name extraction that handles various patterns"""
    # Patterns for customer operations
    customer_patterns = [
        r'delete\s+customer\s+([A-Za-z]+(?:\s+[A-Za-z]+)?)',
        r'remove\s+customer\s+([A-Za-z]+(?:\s+[A-Za-z]+)?)',
        r'update\s+customer\s+([A-Za-z]+(?:\s+[A-Za-z]+)?)',
        r'delete\s+([A-Za-z]+(?:\s+[A-Za-z]+)?)',
        r'remove\s+([A-Za-z]+(?:\s+[A-Za-z]+)?)'
    ]
    
    # Patterns for product operations
    product_patterns = [
        r'delete\s+product\s+([A-Za-z]+(?:\s+[A-Za-z]+)?)',
        r'remove\s+product\s+([A-Za-z]+(?:\s+[A-Za-z]+)?)',
        r'update\s+(?:price\s+of\s+)?([A-Za-z]+(?:\s+[A-Za-z]+)?)',
        r'change\s+price\s+of\s+([A-Za-z]+(?:\s+[A-Za-z]+)?)',
        r'(?:price\s+of\s+)([A-Za-z]+(?:\s+[A-Za-z]+)?)\s+(?:to|=)'
    ]
    
    all_patterns = customer_patterns + product_patterns
    
    for pattern in all_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(1).strip()
    
    return None


def extract_email(text):
    match = re.search(r'[\w\.-]+@[\w\.-]+', text)
    return match.group(0) if match else None


def extract_price(text):
    # Look for price patterns like "to 25", "= 30.50", "$15.99"
    price_patterns = [
        r'to\s+\$?(\d+(?:\.\d+)?)',
        r'=\s+\$?(\d+(?:\.\d+)?)',
        r'\$(\d+(?:\.\d+)?)',
        r'(\d+(?:\.\d+)?)\s*dollars?'
    ]
    
    for pattern in price_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return float(match.group(1))
    
    return None


def generate_table_description(df: pd.DataFrame, content: dict, action: str, tool: str) -> str:
    """Generate LLM-based table description from JSON response data"""

    # Sample first few rows for context (don't send all data to LLM)
    sample_data = df.head(3).to_dict('records') if len(df) > 0 else []

    # Create context for LLM
    context = {
        "action": action,
        "tool": tool,
        "record_count": len(df),
        "columns": list(df.columns) if len(df) > 0 else [],
        "sample_data": sample_data,
        "full_response": content.get("result", [])[:3] if isinstance(content.get("result"), list) else content.get(
            "result", "")
    }

    system_prompt = (
        "You are a data analyst. Generate a brief, insightful 1-line description "
        "of the table data based on the JSON response. Focus on what the data represents "
        "and any interesting patterns you notice. Be concise and business-focused."
    )

    user_prompt = f"""
    Analyze this table data and generate a single insightful line about it:

    Context: {json.dumps(context, indent=2)}

    Generate one line describing what this data represents and any key insights.
    """

    try:
        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_prompt)
        ]
        response = groq_client.invoke(messages)
        return response.content.strip()
    except Exception as e:
        return f"Retrieved {len(df)} records from the database."


# ========== MAIN ==========
if application == "MCP Application":
    user_avatar_url = "https://cdn-icons-png.flaticon.com/512/1946/1946429.png"
    agent_avatar_url = "https://cdn-icons-png.flaticon.com/512/4712/4712039.png"

    MCP_SERVER_URL = os.getenv("MCP_SERVER_URL", "http://localhost:8000")
    st.session_state["MCP_SERVER_URL"] = MCP_SERVER_URL

    # Discover tools dynamically if not already done
    if not st.session_state.available_tools:
        with st.spinner("Discovering available tools..."):
            discovered_tools = discover_tools()
            st.session_state.available_tools = discovered_tools
            st.session_state.tool_states = {tool: True for tool in discovered_tools.keys()}

    # Generate dynamic tool descriptions
    TOOL_DESCRIPTIONS = generate_tool_descriptions(st.session_state.available_tools)

    # ========== TOOLS STATUS AND REFRESH BUTTON ==========
    # Create columns for tools info and refresh button
    col1, col2 = st.columns([4, 1])

    with col1:
        # Display discovered tools info
        if st.session_state.available_tools:
            st.info(
                f"🔧 Discovered {len(st.session_state.available_tools)} tools: {', '.join(st.session_state.available_tools.keys())}")
        else:
            st.warning("⚠️ No tools discovered. Please check your MCP server connection.")

    with col2:
        # Small refresh button on main page
        st.markdown('<div class="small-refresh-button">', unsafe_allow_html=True)
        if st.button("🔄 Active Server", key="refresh_tools_main", help="Rediscover available tools"):
            with st.spinner("Refreshing tools..."):
                MCP_SERVER_URL = os.getenv("MCP_SERVER_URL", "http://localhost:8000")
                st.session_state["MCP_SERVER_URL"] = MCP_SERVER_URL
                discovered_tools = discover_tools()
                st.session_state.available_tools = discovered_tools
                st.session_state.tool_states = {tool: True for tool in discovered_tools.keys()}
                st.rerun()
        st.markdown('</div>', unsafe_allow_html=True)

    # ========== 1. RENDER CHAT MESSAGES ==========
    st.markdown('<div class="stChatPaddingBottom">', unsafe_allow_html=True)
    for msg in st.session_state.messages:
        if msg["role"] == "user":
            st.markdown(
                f"""
                <div class="chat-row right">
                    <div class="chat-bubble user-msg user-bubble">{msg['content']}</div>
                    <img src="{user_avatar_url}" class="avatar user-avatar" alt="User">
                </div>
                """,
                unsafe_allow_html=True,
            )
        elif msg.get("format") == "reasoning":
            st.markdown(
                f"""
                <div class="chat-row left">
                    <img src="{agent_avatar_url}" class="avatar agent-avatar" alt="Agent">
                    <div class="chat-bubble agent-msg agent-bubble"><i>{msg['content']}</i></div>
                </div>
                """,
                unsafe_allow_html=True,
            )
        elif msg.get("format") == "multi_step_read" and isinstance(msg["content"], dict):
            step = msg["content"]
            st.markdown(
                f"""
                <div class="chat-row left">
                    <img src="{agent_avatar_url}" class="avatar agent-avatar" alt="Agent">
                    <div class="chat-bubble agent-msg agent-bubble">
                        <b>Step: Lookup by name</b> (<code>{step['args'].get('name', '')}</code>)
                    </div>
                </div>
                """, unsafe_allow_html=True
            )
            with st.expander(f"Lookup Request: {step['tool']}"):
                st.code(json.dumps({
                    "tool": step['tool'],
                    "action": step['action'],
                    "args": step['args']
                }, indent=2), language="json")
            if isinstance(step["result"], dict) and "sql" in step["result"]:
                with st.expander("Lookup SQL Query Used"):
                    st.code(step["result"]["sql"], language="sql")
            if isinstance(step["result"], dict) and "result" in step["result"]:
                with st.expander("Lookup Response"):
                    st.code(json.dumps(step["result"]["result"], indent=2), language="json")
                    if isinstance(step["result"]["result"], list) and step["result"]["result"]:
                        df = pd.DataFrame(step["result"]["result"])
                        st.markdown("**Lookup Result Table:**")
                        st.table(df)
        elif msg.get("format") == "sql_crud" and isinstance(msg["content"], dict):
            content = msg["content"]
            action = msg.get("action", "")
            tool = msg.get("tool", "")
            user_query = msg.get("user_query", "")

            with st.expander("Details"):
                if "request" in msg:
                    st.markdown("**Request**")
                    st.code(json.dumps(msg["request"], indent=2), language="json")
                    st.markdown("---")
                st.markdown("**SQL Query Used**")
                st.code(content["sql"] or "No SQL executed", language="sql")
                st.markdown("---")
                st.markdown("**Response**")
                if isinstance(content["result"], (dict, list)):
                    st.code(json.dumps(content["result"], indent=2), language="json")
                else:
                    st.code(content["result"])

            # Generate LLM response for the operation
            llm_response = generate_llm_response(content, action, tool, user_query)

            st.markdown(
                f"""
                <div class="chat-row left">
                    <img src="{agent_avatar_url}" class="avatar agent-avatar" alt="Agent">
                    <div class="chat-bubble agent-msg agent-bubble">{llm_response}</div>
                </div>
                """, unsafe_allow_html=True
            )

            if action in {"create", "update", "delete"}:
                result_msg = content.get("result", "")
                if "✅" in result_msg or "success" in result_msg.lower():
                    st.success(result_msg)
                elif "❌" in result_msg or "fail" in result_msg.lower() or "error" in result_msg.lower():
                    st.error(result_msg)
                else:
                    st.info(result_msg)
                try:
                    st.markdown("#### Here's the updated table after your operation:")
                    read_tool = tool
                    read_args = {}
                    updated_table = call_mcp_tool(read_tool, "read", read_args)
                    if isinstance(updated_table, dict) and "result" in updated_table:
                        updated_df = pd.DataFrame(updated_table["result"])
                        st.table(updated_df)
                except Exception as fetch_err:
                    st.info(f"Could not retrieve updated table: {fetch_err}")

            if action == "read" and isinstance(content["result"], list):
                st.markdown("#### Here's the current table:")
                df = pd.DataFrame(content["result"])
                st.table(df)
                # Check if this is ETL formatted data by looking for specific formatting
                if tool == "sales_crud" and len(df.columns) > 0:
                    # Check for different ETL formats based on column names
                    if "sale_summary" in df.columns:
                        st.info("📊 Data formatted with String Concatenation - Combined fields for readability")
                    elif "sale_date" in df.columns and isinstance(df["sale_date"].iloc[0] if len(df) > 0 else None,
                                                                  str):
                        st.info("📅 Data formatted with Data Format Conversion - Dates converted to string format")
                    elif any(
                            "." in str(val) and len(str(val).split(".")[-1]) == 2 for val in df.get("unit_price", []) if
                            pd.notna(val)):
                        st.info("💰 Data formatted with Decimal Value Formatting - Prices formatted to 2 decimal places")
                    else:
                        st.markdown(f"The table contains {len(df)} sales records with cross-database information.")
                elif tool == "sqlserver_crud":
                    st.markdown(
                        f"The table contains {len(df)} customers with their respective IDs, names, emails, and creation timestamps."
                    )
                elif tool == "postgresql_crud":
                    st.markdown(
                        f"The table contains {len(df)} products with their respective IDs, names, prices, and descriptions."
                    )
                else:
                    st.markdown(f"The table contains {len(df)} records.")
            elif action == "describe" and isinstance(content['result'], list):
                st.markdown("#### Table Schema: ")
                df = pd.DataFrame(content['result'])
                st.table(df)
                st.markdown(
                    "This shows the column names, data types, nullability, and maximum length for each column in the table.")
        else:
            st.markdown(
                f"""
                <div class="chat-row left">
                    <img src="{agent_avatar_url}" class="avatar agent-avatar" alt="Agent">
                    <div class="chat-bubble agent-msg agent-bubble">{msg['content']}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )
    st.markdown('</div>', unsafe_allow_html=True)  # End stChatPaddingBottom

    # ========== 2. CLAUDE-STYLE STICKY CHAT BAR ==========
    st.markdown('<div class="sticky-chatbar"><div class="chatbar-claude">', unsafe_allow_html=True)
    with st.form("chatbar_form", clear_on_submit=True):
        chatbar_cols = st.columns([1, 16, 1])  # Left: hamburger, Middle: input, Right: send

        # --- LEFT: Hamburger (Tools) ---
        with chatbar_cols[0]:
            hamburger_clicked = st.form_submit_button("≡", use_container_width=True)

        # --- MIDDLE: Input Box ---
        with chatbar_cols[1]:
            user_query_input = st.text_input(
                "Chat Input",  # Provide a label
                placeholder="How can I help you today?",
                label_visibility="collapsed",  # Hide the label visually
                key="chat_input_box"
            )

        # --- RIGHT: Send Button ---
        with chatbar_cols[2]:
            send_clicked = st.form_submit_button("➤", use_container_width=True)
    st.markdown('</div></div>', unsafe_allow_html=True)

    # ========== FLOATING TOOL MENU ==========
    if st.session_state.get("show_menu", False):
        st.markdown('<div class="tool-menu">', unsafe_allow_html=True)
        st.markdown('<div class="server-title">MultiDBCRUD</div>', unsafe_allow_html=True)
        tool_label = "Tools" + (" ▼" if st.session_state["menu_expanded"] else " ▶")
        if st.button(tool_label, key="expand_tools", help="Show tools", use_container_width=True):
            st.session_state["menu_expanded"] = not st.session_state["menu_expanded"]
        if st.session_state["menu_expanded"]:
            st.markdown('<div class="expandable">', unsafe_allow_html=True)
            for tool in st.session_state.tool_states.keys():
                enabled = st.session_state.tool_states[tool]
                new_val = st.toggle(tool, value=enabled, key=f"tool_toggle_{tool}")
                st.session_state.tool_states[tool] = new_val
            st.markdown('</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

    # ========== HANDLE HAMBURGER ==========
    if hamburger_clicked:
        st.session_state["show_menu"] = not st.session_state.get("show_menu", False)
        st.rerun()

    # ========== PROCESS CHAT INPUT ==========
    if send_clicked and user_query_input:
        user_query = user_query_input
        user_steps = []
        try:
            enabled_tools = [k for k, v in st.session_state.tool_states.items() if v]
            if not enabled_tools:
                raise Exception("No tools are enabled. Please enable at least one tool in the menu.")

            p = parse_user_query(user_query, st.session_state.available_tools)
            tool = p.get("tool")
            if tool not in enabled_tools:
                raise Exception(f"Tool '{tool}' is disabled. Please enable it in the menu.")
            if tool not in st.session_state.available_tools:
                raise Exception(
                    f"Tool '{tool}' is not available. Available tools: {', '.join(st.session_state.available_tools.keys())}")

            action = p.get("action")
            args = p.get("args", {})

            # VALIDATE AND CLEAN PARAMETERS
            args = validate_and_clean_parameters(tool, args)
            args = normalize_args(args)
            p["args"] = args

            # ========== ENHANCED NAME-BASED RESOLUTION ==========
            
            # For SQL Server (customers) operations
            if tool == "sqlserver_crud":
                if action in ["update", "delete"] and "name" in args and "customer_id" not in args:
                    # First, try to find the customer by name
                    name_to_find = args["name"]
                    try:
                        # Search for customer by name
                        read_result = call_mcp_tool(tool, "read", {})
                        if isinstance(read_result, dict) and "result" in read_result:
                            customers = read_result["result"]
                            # Try exact match first
                            exact_matches = [c for c in customers if c.get("Name", "").lower() == name_to_find.lower()]
                            if exact_matches:
                                args["customer_id"] = exact_matches[0]["Id"]
                            else:
                                # Try partial matches (first name or last name)
                                partial_matches = [c for c in customers if 
                                    name_to_find.lower() in c.get("Name", "").lower() or
                                    name_to_find.lower() in c.get("FirstName", "").lower() or 
                                    name_to_find.lower() in c.get("LastName", "").lower()]
                                if partial_matches:
                                    args["customer_id"] = partial_matches[0]["Id"]
                                else:
                                    raise Exception(f"❌ Customer '{name_to_find}' not found")
                    except Exception as e:
                        if "not found" in str(e):
                            raise e
                        else:
                            raise Exception(f"❌ Error finding customer '{name_to_find}': {str(e)}")

                # Extract new email for updates
                if action == "update" and "new_email" not in args:
                    possible_email = extract_email(user_query)
                    if possible_email:
                        args["new_email"] = possible_email

            # For PostgreSQL (products) operations  
            elif tool == "postgresql_crud":
                if action in ["update", "delete"] and "name" in args and "product_id" not in args:
                    # First, try to find the product by name
                    name_to_find = args["name"]
                    try:
                        # Search for product by name
                        read_result = call_mcp_tool(tool, "read", {})
                        if isinstance(read_result, dict) and "result" in read_result:
                            products = read_result["result"]
                            # Try exact match first
                            exact_matches = [p for p in products if p.get("name", "").lower() == name_to_find.lower()]
                            if exact_matches:
                                args["product_id"] = exact_matches[0]["id"]
                            else:
                                # Try partial matches
                                partial_matches = [p for p in products if name_to_find.lower() in p.get("name", "").lower()]
                                if partial_matches:
                                    args["product_id"] = partial_matches[0]["id"]
                                else:
                                    raise Exception(f"❌ Product '{name_to_find}' not found")
                    except Exception as e:
                        if "not found" in str(e):
                            raise e
                        else:
                            raise Exception(f"❌ Error finding product '{name_to_find}': {str(e)}")

                # Extract new price for updates
                if action == "update" and "new_price" not in args:
                    possible_price = extract_price(user_query)
                    if possible_price is not None:
                        args['new_price'] = possible_price

            # Update the parsed args
            p["args"] = args

            # Handle describe operations
            if action == "describe" and "table_name" in args:
                if tool == "sqlserver_crud" and args["table_name"].lower() in ["customer", "customer table"]:
                    args["table_name"] = "Customers"
                if tool == "postgresql_crud" and args["table_name"].lower() in ["product", "product table"]:
                    args["table_name"] = "products"

            raw = call_mcp_tool(p["tool"], p["action"], p.get("args", {}))
        except Exception as e:
            reply, fmt = f"⚠️ Error: {e}", "text"
            assistant_message = {
                "role": "assistant",
                "content": reply,
                "format": fmt,
            }
            st.session_state.messages.append({
                "role": "user",
                "content": user_query,
                "format": "text",
            })
            st.session_state.messages.append(assistant_message)
        else:
            st.session_state.messages.append({
                "role": "user",
                "content": user_query,
                "format": "text",
            })
            for step in user_steps:
                st.session_state.messages.append(step)
            if isinstance(raw, dict) and "sql" in raw and "result" in raw:
                reply, fmt = raw, "sql_crud"
            else:
                reply, fmt = format_natural(raw), "text"
            assistant_message = {
                "role": "assistant",
                "content": reply,
                "format": fmt,
                "request": p,
                "tool": p.get("tool"),
                "action": p.get("action"),
                "args": p.get("args"),
                "user_query": user_query,  # Added user_query to the message
            }
            st.session_state.messages.append(assistant_message)
        st.rerun()  # Rerun so chat output appears

    # ========== 4. AUTO-SCROLL TO BOTTOM ==========
    components.html("""
        <script>
          setTimeout(() => { window.scrollTo(0, document.body.scrollHeight); }, 80);
        </script>
    """)

# ========== ETL EXAMPLES HELP SECTION ==========
with st.expander("🔧 ETL Functions & Examples"):
    st.markdown("""
    ### ETL Display Formatting Functions

    Your MCP server supports 4 ETL (Extract, Transform, Load) functions for data formatting:

    #### 1. Data Format Conversion
    - **Query Examples:** 
      - "show sales with data format conversion"
      - "convert sales data format"
      - "format sales data for export"
    - **What it does:** Converts dates to string format, removes unnecessary fields

    #### 2. Decimal Value Formatting  
    - **Query Examples:**
      - "format sales prices with decimal formatting" 
      - "show sales with 2 decimal places"
      - "decimal value formatting for sales"
    - **What it does:** Formats all prices to exactly 2 decimal places as strings

    #### 3. String Concatenation
    - **Query Examples:**
      - "combine sales fields for readability"
      - "show sales with concatenated fields"
    - **What it does:** Creates readable summary fields by combining related data

    #### 4. Null Value Removal/Handling
    - **Query Examples:**
      - "clean sales data with null handling"
      - "remove nulls from sales data"
      - "handle null values in sales"
    - **What it does:** Filters out incomplete records and handles null values

    ### Regular Operations
    - **"list all sales"** - Shows regular unformatted sales data
    - **"show customers"** - Shows customer data
    - **"list products"** - Shows product inventory
    
    ### Smart Name-Based Operations (NEW!)
    - **"delete customer Alice"** - Finds and deletes Alice by name
    - **"delete Alice Johnson"** - Finds customer by full name
    - **"remove Johnson"** - Finds customer by last name
    - **"delete product Widget"** - Finds and deletes Widget by name
    - **"update price of Gadget to 25"** - Updates Gadget price to $25
    - **"change email of Bob to bob@new.com"** - Updates Bob's email
    """)

