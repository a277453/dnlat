"""
Individual Transaction UI Flow Visualization Module

This module creates Plotly-based interactive flowcharts for individual transactions,
showing the sequence of screens and events with timestamps.
"""

import plotly.graph_objects as go
from typing import Dict, List, Tuple, Optional
import re
from datetime import datetime
from modules.logging_config import logger
import logging

logger.info("Individual Transaction UI Flow Visualization Module loaded")



def create_individual_transaction_flow_plotly(
    transaction_id: str,
    transaction_type: str,
    start_time: str,
    end_time: str,
    ui_flow: List[str],
    transaction_log: Optional[str] = None
) -> go.Figure:
    """
    Create a Plotly-based vertical flowchart for an individual transaction
    """
    logger.info(f"Creating transaction flow figure for ID: {transaction_id}")

    try:
        # Extract screens with details from transaction log if available
        screens_with_details = []
        if transaction_log and ui_flow and ui_flow[0] != 'No flow data':
            logger.debug("Extracting screen details from transaction log.")
            screens_with_details = _extract_screens_from_log(transaction_log, ui_flow)
        elif ui_flow and ui_flow[0] != 'No flow data':
            logger.debug("Using UI flow data without detailed parsing.")
            screens_with_details = [(screen, "", "OK") for screen in ui_flow]
        else:
            logger.warning("No UI flow data available.")
            screens_with_details = [("No flow data available", "", "")]

        # Calculate event count and dates
        num_events = len(screens_with_details)
        logger.debug(f"Number of events/screens: {num_events}")
        current_date = datetime.now().strftime("%Y-%m-%d")

        # Create the Plotly figure
        fig = go.Figure()
        box_width = 500
        box_height = 80
        spacing = 40
        y_start = 100

        # Add title header
        fig.add_annotation(
            x=350,
            y=y_start + 80,
            text=f"<b>Transaction Flow: {transaction_id}</b><br>",
            showarrow=False,
            font=dict(size=20, color='#0d47a1', family='Arial Black'),
            bgcolor='#e3f2fd',
            bordercolor='#1976d2',
            borderwidth=2,
            borderpad=10,
            xanchor='center'
        )

        # Draw each screen box
        y_position = y_start - 50
        for i, (screen, timestamp, result_detail) in enumerate(screens_with_details):
            y_position -= (box_height + spacing)

            if any(term in screen.lower() for term in ['error', 'fail', 'cancel', 'timeout']):
                box_color = '#ffcdd2'  # Red
                logger.debug(f"Screen '{screen}' marked as error/fail/cancel/timeout.")
            else:
                box_color = '#bbdefb'  # Blue

            # Add step number circle
            fig.add_shape(
                type="circle",
                x0=30, x1=70,
                y0=y_position - 20, y1=y_position + 20,
                fillcolor='#1976d2',
                line=dict(color='white', width=2),
                layer='above'
            )

            fig.add_annotation(
                x=50,
                y=y_position,
                text=f"<b>{i+1}</b>",
                showarrow=False,
                font=dict(size=14, color='white', family='Arial'),
                xanchor='center',
                yanchor='middle'
            )

            # Add main screen box
            fig.add_shape(
                type="rect",
                x0=100, x1=100 + box_width,
                y0=y_position - box_height//2,
                y1=y_position + box_height//2,
                fillcolor=box_color,
                line=dict(color='#1976d2', width=2),
                layer='below'
            )

            # Add screen name with timestamp
            screen_text = f"<b>{screen}</b>"
            if timestamp:
                screen_text += f" [{timestamp}]"

            fig.add_annotation(
                x=350,
                y=y_position + 15,
                text=screen_text,
                showarrow=False,
                font=dict(size=14, color='#0d47a1', family='Arial'),
                xanchor='center'
            )

            # Add result detail
            if result_detail:
                fig.add_annotation(
                    x=350,
                    y=y_position - 15,
                    text=f"<i>Result: {result_detail}</i>",
                    showarrow=False,
                    font=dict(size=10, color='#2e7d32', family='Arial'),
                    xanchor='center'
                )

            # Add connecting arrow (except for last step)
            if i < len(screens_with_details) - 1:
                arrow_y_start = y_position - box_height//2 - 5
                arrow_y_end = arrow_y_start - spacing + 10

                fig.add_annotation(
                    x=350,
                    y=arrow_y_end,
                    ax=350,
                    ay=arrow_y_start,
                    xref='x', yref='y',
                    axref='x', ayref='y',
                    showarrow=True,
                    arrowhead=2,
                    arrowsize=1.5,
                    arrowwidth=2.5,
                    arrowcolor='#2e7d32'
                )

        total_height = abs(y_position) + 150
        fig.update_layout(
            width=700,
            height=min(total_height, 2000),
            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False, range=[-50, 650]),
            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False, range=[y_position - 100, y_start + 150]),
            plot_bgcolor='white',
            paper_bgcolor='white',
            margin=dict(t=20, l=20, r=20, b=20),
            hovermode='closest'
        )

        logger.info(f"Transaction flow figure created successfully for ID: {transaction_id}")
        return fig

    except Exception as e:
        logger.error(f"Failed to create transaction flow for ID {transaction_id}: {e}")
        raise


def _extract_screens_from_log(transaction_log: str, ui_flow: List[str]) -> List[Tuple[str, str, str]]:
    """
    Extract screen information with timestamps and results from transaction log
    """
    logger.debug("Extracting screens from transaction log.")
    screens_with_details = []
    log_lines = transaction_log.split('\n')
    screen_set = set(ui_flow)

    for line in log_lines:
        line = line.strip()
        if not line:
            continue

        timestamp_match = re.match(r'^(\d{2}:\d{2}:\d{2})', line)
        if not timestamp_match:
            continue

        timestamp = timestamp_match.group(1)

        for screen in screen_set:
            if screen in line:
                result_detail = "OK"
                if 'CANCEL' in line.upper():
                    result_detail = "CANCEL"
                elif 'DISPLAY' in line.upper():
                    result_detail = "DISPLAY"
                elif 'ERROR' in line.upper():
                    result_detail = "ERROR"
                elif 'SUCCESS' in line.upper():
                    result_detail = "SUCCESS"
                elif 'TIMEOUT' in line.upper():
                    result_detail = "TIMEOUT"

                screens_with_details.append((screen, timestamp, result_detail))
                screen_set.remove(screen)
                logger.debug(f"Screen matched: {screen}, Result: {result_detail}, Timestamp: {timestamp}")
                break

        if not screen_set:
            break

    for screen in screen_set:
        screens_with_details.append((screen, "", "OK"))
        logger.debug(f"Screen not found in log, default added: {screen}")

    logger.info(f"Extracted {len(screens_with_details)} screens from transaction log.")
    return screens_with_details


def create_individual_flow_from_ui_data(
    transaction_data: Dict,
    ui_flow_screens: List[str]
) -> go.Figure:
    """
    Convenience function to create flow from transaction data dict
    """
    transaction_id = transaction_data.get('Transaction ID', 'Unknown')
    logger.info(f"Creating flow from UI data for Transaction ID: {transaction_id}")
    try:
        fig = create_individual_transaction_flow_plotly(
            transaction_id=transaction_id,
            transaction_type=transaction_data.get('Transaction Type', 'Unknown'),
            start_time=str(transaction_data.get('Start Time', '')),
            end_time=str(transaction_data.get('End Time', '')),
            ui_flow=ui_flow_screens,
            transaction_log=transaction_data.get('Transaction Log', '')
        )
        logger.info(f"Flow figure created successfully for Transaction ID: {transaction_id}")
        return fig
    except Exception as e:
        logger.error(f"Failed to create flow from UI data for Transaction ID {transaction_id}: {e}")
        raise