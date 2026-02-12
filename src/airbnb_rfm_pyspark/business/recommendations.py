from dataclasses import dataclass
from typing import Dict, List


@dataclass
class MarketingAction:
    """Structure of a recommended marketing action."""

    channel: str  # Email, SMS, Push, Display, Direct Mail
    offer: str  # Type of offer (kept in French for customer-facing copy)
    message: str  # Message template (kept in French for customer-facing copy)
    priority: int  # 1 (high) to 3 (low)
    expected_roi_label: str  # Qualitative label: HIGH / MEDIUM / LOW
    # Quantitative assumptions
    conversion_rate: float  # Estimated conversion rate for the campaign
    basket_multiplier: float  # Multiplier on global average basket value
    discount_rate: float  # Average discount rate applied on basket
    channels_cost_per_user: float  # Media + sending cost per targeted user


class MarketingRecommendations:
    """
    Generator for marketing recommendations by segment.

    Business Context:
    - Champions: Maximize retention with perks, limited discounts
    - Loyal: Upselling and referral programs with moderate incentives
    - Potential: Conversion with attractive incentives
    - At Risk: Urgent reactivation campaigns with strong offers
    - Hibernating: Win-back with aggressive promotions, volume play
    - Lost: Low-cost reactivation or deprioritization

    Technical assumptions:
    - "M" ~ contribution revenue (approximated here by avg basket * multiplier).[web:2][web:21]
    - ROI = (Incremental revenue - Campaign cost) / Campaign cost.[web:20][web:22]
    """

    # Base average basket (e.g. Airbnb Pays Basque 3 nights * 100€/night)
    BASE_AVG_BASKET = 300.0

    # Per-channel unit costs (hypotheses; adapt to your context)
    CHANNEL_UNIT_COSTS = {
        "email": 0.002,  # 0.2 cent per email
        "sms": 0.05,  # 5 cents per SMS
        "push": 0.0,  # marginal cost close to zero
        "display": 0.02,  # averaged CPM/CPC per user
        "direct_mail": 0.7,  # printing + postage
    }

    @classmethod
    def _compute_channels_cost(cls, channels_str: str) -> float:
        """
        Compute average cost per user based on the combination of channels.

        Example:
        "Email VIP + Push Notification" => email cost + push cost.
        """
        channels = channels_str.lower()
        cost = 0.0
        if "email" in channels:
            cost += cls.CHANNEL_UNIT_COSTS["email"]
        if "sms" in channels:
            cost += cls.CHANNEL_UNIT_COSTS["sms"]
        if "push" in channels:
            cost += cls.CHANNEL_UNIT_COSTS["push"]
        if "display" in channels or "retargeting" in channels:
            cost += cls.CHANNEL_UNIT_COSTS["display"]
        if "direct mail" in channels:
            cost += cls.CHANNEL_UNIT_COSTS["direct_mail"]
        return cost

    @classmethod
    def get_recommendations(cls, segment: str) -> MarketingAction:
        """
        Return the recommended marketing action for a given segment.

        NOTE: in production, these parameters would come from a rule engine
        and A/B tests, fed by real performance per segment.[web:2][web:21]
        """

        strategies: Dict[str, Dict] = {
            "Champions": {
                "channel": "Email VIP + Push Notification",
                "offer": (
                    "Programme de fidélité exclusif : Early Access + perks, "
                    "remise limitée (-10%)"
                ),
                "message": (
                    "Merci d'être un voyageur d'exception ! "
                    "Profitez d'avantages exclusifs et d'un accès prioritaire aux nouveaux logements."
                ),
                "priority": 1,
                "expected_roi_label": "HIGH (4–6x)",
                "conversion_rate": 0.20,  # 20% on a multi-touch sequence for highly engaged users[web:2][web:21]
                "basket_multiplier": 1.20,  # basket +20%
                "discount_rate": 0.10,
            },
            "Loyal": {
                "channel": "Email personnalisé + SMS",
                "offer": (
                    "Parrainage récompensé : 30€ offerts par filleul "
                    "+ paiement en 3x sans frais"
                ),
                "message": (
                    "Partagez votre expérience ! Pour chaque ami parrainé, "
                    "recevez 30€ et offrez-lui 20€ de réduction."
                ),
                "priority": 1,
                "expected_roi_label": "HIGH (3–5x)",
                "conversion_rate": 0.15,  # 15% realistic for warm base via email+SMS[web:19][web:21]
                "basket_multiplier": 1.10,  # basket +10%
                "discount_rate": 0.12,
            },
            "Potential": {
                "channel": "Email + Retargeting Display",
                "offer": (
                    "Code promo -15% + frais de service offerts " "sur 2e réservation"
                ),
                "message": (
                    "Vous avez aimé votre séjour ? "
                    "Réservez à nouveau avec -15% et sans frais de service !"
                ),
                "priority": 2,
                "expected_roi_label": "MEDIUM (2–4x)",
                "conversion_rate": 0.10,  # 10% on properly segmented email + retargeting[web:21][web:27]
                "basket_multiplier": 1.00,
                "discount_rate": 0.15,
            },
            "At Risk": {
                "channel": "Email Win-Back + SMS Urgent",
                "offer": (
                    "Offre de reconquête : -20% + annulation gratuite " "jusqu'à J-1"
                ),
                "message": (
                    "Ça fait longtemps ! Nous avons amélioré notre service. "
                    "Revenez avec -20% et une flexibilité maximale."
                ),
                "priority": 1,
                "expected_roi_label": "MEDIUM (2–3x)",
                "conversion_rate": 0.10,  # close to strong automated win-back benchmarks (~10%)[web:13][web:27]
                "basket_multiplier": 0.95,  # slightly lower basket
                "discount_rate": 0.20,
            },
            "Hibernating": {
                "channel": "Email Automation + Display Remarketing",
                "offer": ("Réactivation : -25% + bon cadeau 30€ utilisable en 2 fois"),
                "message": (
                    "Le Pays Basque vous manque ? Redécouvrez la ville avec -25% "
                    "et un bon cadeau de 30€ pour vos prochains séjours."
                ),
                "priority": 2,
                "expected_roi_label": "LOW–MEDIUM (1.5–2.5x)",
                "conversion_rate": 0.07,
                "basket_multiplier": 0.90,  # basket -10%
                "discount_rate": 0.25,
            },
            "Lost": {
                "channel": "Email low-cost (bulk)",
                "offer": "Newsletter générique + offres saisonnières (-10%)",
                "message": (
                    "Découvrez les meilleures destinations de la saison avec -10%. "
                    "Offre valable pour une durée limitée."
                ),
                "priority": 3,
                "expected_roi_label": "LOW (<=1.5x)",
                "conversion_rate": 0.03,
                "basket_multiplier": 0.80,  # basket -20%
                "discount_rate": 0.10,
            },
        }

        # Fallback to Lost if segment is unknown
        config = strategies.get(segment, strategies["Lost"])

        channels_cost = cls._compute_channels_cost(config["channel"])

        return MarketingAction(
            channel=config["channel"],
            offer=config["offer"],
            message=config["message"],
            priority=config["priority"],
            expected_roi_label=config["expected_roi_label"],
            conversion_rate=config["conversion_rate"],
            basket_multiplier=config["basket_multiplier"],
            discount_rate=config["discount_rate"],
            channels_cost_per_user=channels_cost,
        )

    @classmethod
    def estimate_campaign_impact(
        cls,
        segment_counts: Dict[str, int],
        gross_margin_rate: float = 0.60,
    ) -> Dict[str, Dict[str, float]]:
        """
        Estimate financial impact of a campaign per segment.

        For each segment, it uses:
        - conversion_rate (segment-level hypothesis)
        - basket_multiplier (avg basket uplift/decline)
        - discount_rate (direct cost of the discount)
        - channels_cost_per_user (media/tech cost per contact)

        For each segment, it returns:
        - revenue: incremental gross revenue
        - margin_before_discount: margin before discount cost
        - discount_cost
        - media_cost
        - total_marketing_cost
        - net_profit
        - roi: (margin_before_discount - discount_cost - media_cost)
               / (discount_cost + media_cost)[web:20][web:22]
        """

        results: Dict[str, Dict[str, float]] = {}

        for segment, count in segment_counts.items():
            action = cls.get_recommendations(segment)

            # Expected incremental revenue
            avg_basket_segment = cls.BASE_AVG_BASKET * action.basket_multiplier
            expected_conversions = count * action.conversion_rate
            revenue = expected_conversions * avg_basket_segment

            # Gross margin approximation (before discount)
            margin_before_discount = revenue * gross_margin_rate

            # Discount cost (direct hit on margin)
            discount_cost = revenue * action.discount_rate

            # Media / sending cost
            media_cost = count * action.channels_cost_per_user

            # Total marketing cost attributed to the campaign
            total_marketing_cost = discount_cost + media_cost

            # Net marketing profit
            net_profit = margin_before_discount - total_marketing_cost

            # Marketing ROI (ROMI)
            if total_marketing_cost > 0:
                roi = net_profit / total_marketing_cost
            else:
                roi = float("inf")

            results[segment] = {
                "revenue": round(revenue, 2),
                "margin_before_discount": round(margin_before_discount, 2),
                "discount_cost": round(discount_cost, 2),
                "media_cost": round(media_cost, 2),
                "total_marketing_cost": round(total_marketing_cost, 2),
                "net_profit": round(net_profit, 2),
                "roi": round(roi, 2),
            }

        return results

    @staticmethod
    def generate_segment_priorities() -> List[Dict]:
        """
        Return segment list ordered by action priority.

        Usage: for dashboards or executive reporting.
        """

        return [
            {
                "segment": "Champions",
                "priority": "CRITICAL",
                "action": "Max retention",
                "budget_allocation": "25%",
            },
            {
                "segment": "At Risk",
                "priority": "URGENT",
                "action": "Immediate win-back",
                "budget_allocation": "20%",
            },
            {
                "segment": "Loyal",
                "priority": "HIGH",
                "action": "Upselling + Referral",
                "budget_allocation": "20%",
            },
            {
                "segment": "Potential",
                "priority": "MEDIUM",
                "action": "Conversion",
                "budget_allocation": "15%",
            },
            {
                "segment": "Hibernating",
                "priority": "MEDIUM",
                "action": "Reactivation",
                "budget_allocation": "10%",
            },
            {
                "segment": "Lost",
                "priority": "LOW",
                "action": "Maintenance",
                "budget_allocation": "10%",
            },
        ]
